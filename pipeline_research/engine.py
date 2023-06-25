from numpy import array, arange
import pandas as pd
from pandas import DataFrame, MultiIndex
from six import (
    iteritems,
)
from toolz import groupby, juxt
from toolz.curried.operator import getitem

from zipline.lib.adjusted_array import ensure_adjusted_array, ensure_ndarray
from zipline.errors import NoFurtherDataError
from zipline.pipeline.engine import default_populate_initial_workspace, PipelineEngine
from zipline.pipeline.term import AssetExists, InputDates, LoadableTerm
from zipline.pipeline.domain import US_EQUITIES, GENERIC
from zipline.pipeline.graph import maybe_specialize
from zipline.pipeline.hooks import DelegatingHooks

from zipline.utils.numpy_utils import (
    as_column,
    repeat_first_axis,
    repeat_last_axis,
)
from zipline.utils.pandas_utils import explode
from zipline.utils.string_formatting import bulleted_list

class ResearchPipelineEngine(PipelineEngine):
    """
    PipelineEngine class that computes each term independently.

    Parameters
    ----------
    get_loader : callable
        A function that is given a loadable term and returns a PipelineLoader
        to use to retrieve raw data for that term.
    asset_finder : zipline_pipeline.assets.AssetFinder
        An AssetFinder instance.  We depend on the AssetFinder to determine
        which assets are in the top-level universe at any point in time.
    populate_initial_workspace : callable, optional
        A function which will be used to populate the initial workspace when
        computing a pipeline. See
        :func:`zipline_pipeline.pipeline.engine.default_populate_initial_workspace`
        for more info.
    default_hooks : list, optional
        List of hooks that should be used to instrument all pipelines executed
        by this engine.

    See Also
    --------
    :func:`zipline_pipeline.pipeline.engine.default_populate_initial_workspace`
    """

    def __init__(self,
                 list_symbols,
                 default_domain=US_EQUITIES,
                 populate_initial_workspace=None,
                 default_hooks=None,
                 ):
        # Instead of get_loader, asset_finder we implement list_symbols
        # self._get_loader = get_loader
        # self._finder = asset_finder
        self._list_symbols = list_symbols

        self._root_mask_term = AssetExists()
        self._root_mask_dates_term = InputDates()

        self._populate_initial_workspace = (
            populate_initial_workspace or default_populate_initial_workspace
        )
        self._default_domain = default_domain

        if default_hooks is None:
            self._default_hooks = []
        else:
            self._default_hooks = list(default_hooks)

    def run_chunked_pipeline(self,
                             pipeline,
                             start_date,
                             end_date,
                             chunksize,
                             hooks=None):
        """
        Compute values for ``pipeline`` from ``start_date`` to ``end_date``, in
        date chunks of size ``chunksize``.

        Chunked execution reduces memory consumption, and may reduce
        computation time depending on the contents of your pipeline.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to run.
        start_date : pd.Timestamp
            The start date to run the pipeline for.
        end_date : pd.Timestamp
            The end date to run the pipeline for.
        chunksize : int
            The number of days to execute at a time.
        hooks : list[implements(PipelineHooks)], optional
            Hooks for instrumenting Pipeline execution.

        Returns
        -------
        result : pd.DataFrame
            A frame of computed results.

            The ``result`` columns correspond to the entries of
            `pipeline.columns`, which should be a dictionary mapping strings to
            instances of :class:`zipline_pipeline.pipeline.Term`.

            For each date between ``start_date`` and ``end_date``, ``result``
            will contain a row for each asset that passed `pipeline.screen`.
            A screen of ``None`` indicates that a row should be returned for
            each asset that existed each day.

        See Also
        --------
        :meth:`zipline_pipeline.pipeline.engine.PipelineEngine.run_pipeline`
        """
        from zipline_pipeline.utils.date_utils import compute_date_range_chunks
        from zipline_pipeline.utils.pandas_utils import categorical_df_concat
        from functools import partial
        domain = self.resolve_domain(pipeline)
        ranges = compute_date_range_chunks(
            domain.all_sessions(),
            start_date,
            end_date,
            chunksize,
        )
        hooks = self._resolve_hooks(hooks)

        run_pipeline = partial(self._run_pipeline_impl, pipeline, hooks=hooks)
        with hooks.running_pipeline(pipeline, start_date, end_date):
            chunks = [run_pipeline(s, e) for s, e in ranges]

        if len(chunks) == 1:
            # OPTIMIZATION: Don't make an extra copy in `categorical_df_concat`
            # if we don't have to.
            return chunks[0]

        # Filter out empty chunks. Empty dataframes lose dtype information,
        # which makes concatenation fail.
        nonempty_chunks = [c for c in chunks if len(c)]
        return categorical_df_concat(nonempty_chunks, inplace=True)

    def run_pipeline(self, pipeline, start_date, end_date, hooks=None):
        """
        Compute values for ``pipeline`` from ``start_date`` to ``end_date``.

        Parameters
        ----------
        pipeline : zipline_pipeline.pipeline.Pipeline
            The pipeline to run.
        start_date : pd.Timestamp
            Start date of the computed matrix.
        end_date : pd.Timestamp
            End date of the computed matrix.
        hooks : list[implements(PipelineHooks)], optional
            Hooks for instrumenting Pipeline execution.

        Returns
        -------
        result : pd.DataFrame
            A frame of computed results.

            The ``result`` columns correspond to the entries of
            `pipeline.columns`, which should be a dictionary mapping strings to
            instances of :class:`zipline_pipeline.pipeline.Term`.

            For each date between ``start_date`` and ``end_date``, ``result``
            will contain a row for each asset that passed `pipeline.screen`.
            A screen of ``None`` indicates that a row should be returned for
            each asset that existed each day.
        """
        hooks = self._resolve_hooks(hooks)
        with hooks.running_pipeline(pipeline, start_date, end_date):
            return self._run_pipeline_impl(
                pipeline,
                start_date,
                end_date,
                hooks,
            )

    def _run_pipeline_impl(self, pipeline, start_date, end_date, hooks):
        """Shared core for ``run_pipeline`` and ``run_chunked_pipeline``.
        """
        # See notes at the top of this module for a description of the
        # algorithm implemented here.
        start_date = pd.Timestamp(start_date, tz='UTC')
        end_date = pd.Timestamp(end_date, tz='UTC')
        if end_date < start_date:
            raise ValueError(
                "start_date must be before or equal to end_date \n"
                "start_date=%s, end_date=%s" % (start_date, end_date)
            )

        domain = self.resolve_domain(pipeline)

        graph = pipeline.to_execution_plan(domain,
                                           self._root_mask_term,
                                           start_date,
                                           end_date,
                                           )
        extra_rows = graph.extra_rows[self._root_mask_term]
        root_mask = self._compute_root_mask(
            domain, start_date, end_date, extra_rows,
        )
        dates, symbols, root_mask_values = explode(root_mask)

        workspace = self._populate_initial_workspace(
            {
                self._root_mask_term: root_mask_values,
                self._root_mask_dates_term: as_column(dates.values)
            },
            self._root_mask_term,
            graph,
            dates,
            symbols,
        )

        refcounts = graph.initial_refcounts(workspace)
        execution_order = graph.execution_order(workspace, refcounts)

        with hooks.computing_chunk(execution_order,
                                   start_date,
                                   end_date):
            results = self.compute_chunk(
                graph=graph,
                dates=dates,
                symbols=symbols,
                workspace=workspace,
                refcounts=refcounts,
                execution_order=execution_order,
                hooks=hooks,
            )

        return self._to_narrow(
            graph.outputs,
            results,
            results.pop(graph.screen_name),
            dates[extra_rows:],
            symbols,
        )

    def _compute_root_mask(self, domain, start_date, end_date, extra_rows):
        """
        Compute a lifetimes matrix from our AssetFinder, then drop columns that
        didn't exist at all during the query dates.

        Parameters
        ----------
        domain : zipline_pipeline.pipeline.domain.Domain
            Domain for which we're computing a pipeline.
        start_date : pd.Timestamp
            Base start date for the matrix.
        end_date : pd.Timestamp
            End date for the matrix.
        extra_rows : int
            Number of extra rows to compute before `start_date`.
            Extra rows are needed by terms like moving averages that require a
            trailing window of data.

        Returns
        -------
        lifetimes : pd.DataFrame
            Frame of dtype `bool` containing dates from `extra_rows` days
            before `start_date`, continuing through to `end_date`.  The
            returned frame contains as columns all assets in our AssetFinder
            that existed for at least one day between `start_date` and
            `end_date`.
        """
        sessions = domain.all_sessions()

        # Allow for user to start/end from non-trading days, auto then shrink.
        # if start_date not in sessions:
        #     raise ValueError(
        #         f"Pipeline start date ({start_date}) is not a trading session for "
        #         f"domain {domain}."
        #     )

        # elif end_date not in sessions:
        #     raise ValueError(
        #         f"Pipeline end date {end_date} is not a trading session for "
        #         f"domain {domain}."
        #     )
        
        start_idx, end_idx = sessions.slice_locs(start_date, end_date)
        if start_idx < extra_rows:
            raise NoFurtherDataError.from_lookback_window(
                initial_message="Insufficient data to compute Pipeline:",
                first_date=sessions[0],
                lookback_start=start_date,
                lookback_length=extra_rows,
            )

        # NOTE: This logic should probably be delegated to the domain once we
        #       start adding more complex domains.
        #
        # Build lifetimes matrix reaching back to `extra_rows` days before
        # `start_date.`
        # finder = self._finder
        # lifetimes = finder.lifetimes(
        #     sessions[start_idx - extra_rows:end_idx],
        #     include_start_date=False,
        #     country_codes=(domain.domain_code,),
        # )
        symbols = self._list_symbols()
        symbols = [s for s in symbols if isinstance(s, str)]
        dates = sessions[start_idx - extra_rows:end_idx]
        symbols = sorted(symbols)
        lifetimes = pd.DataFrame(True, index=dates, columns=symbols)

        assert lifetimes.index[extra_rows] >= start_date
        assert lifetimes.index[-1] <= end_date


        if not lifetimes.columns.unique:
            columns = lifetimes.columns
            duplicated = columns[columns.duplicated()].unique()
            raise AssertionError("Duplicated sids: %d" % duplicated)

        # Filter out columns that didn't exist from the farthest look back
        # window through the end of the requested dates.
        existed = lifetimes.any()
        ret = lifetimes.loc[:, existed]
        # num_assets = ret.shape[1]

        # if num_assets == 0:
        #     raise ValueError(
        #         "Failed to find any assets with domain {!r} that traded "
        #         "between {} and {}.\n"
        #         "This probably means that your asset db is old or that it has "
        #         "incorrect country/exchange metadata.".format(
        #             domain.domain_code, start_date, end_date,
        #         )
        #     )
        shape = ret.shape
        assert shape[0] * shape[1] != 0, 'root mask cannot be empty'

        return ret

    @staticmethod
    def _inputs_for_term(term, workspace, graph, domain, refcounts):
        """
        Compute inputs for the given term.

        This is mostly complicated by the fact that for each input we store as
        many rows as will be necessary to serve **any** computation requiring
        that input.
        """
        offsets = graph.offset
        out = []

        # We need to specialize here because we don't change ComputableTerm
        # after resolving domains, so they can still contain generic terms as
        # inputs.
        specialized = [maybe_specialize(t, domain) for t in term.inputs]

        if term.windowed:
            # If term is windowed, then all input data should be instances of
            # AdjustedArray.
            for input_ in specialized:
                adjusted_array = ensure_adjusted_array(
                    workspace[input_], input_.missing_value,
                )
                out.append(
                    adjusted_array.traverse(
                        window_length=term.window_length,
                        offset=offsets[term, input_],
                        # If the refcount for the input is > 1, we will need
                        # to traverse this array again so we must copy.
                        # If the refcount for the input == 0, this is the last
                        # traversal that will happen so we can invalidate
                        # the AdjustedArray and mutate the data in place.
                        copy=refcounts[input_] > 1,
                    )
                )
        else:
            # If term is not windowed, input_data may be an AdjustedArray or
            # np.ndarray. Coerce the former to the latter.
            for input_ in specialized:
                input_data = ensure_ndarray(workspace[input_])
                offset = offsets[term, input_]
                input_data = input_data[offset:]
                if refcounts[input_] > 1:
                    input_data = input_data.copy()
                out.append(input_data)
        return out

    def compute_chunk(self,
                      graph,
                      dates,
                      symbols,
                      workspace,
                      refcounts,
                      execution_order,
                      hooks):
        """
        Compute the Pipeline terms in the graph for the requested start and end
        dates.

        This is where we do the actual work of running a pipeline.

        Parameters
        ----------
        graph : zipline_pipeline.pipeline.graph.ExecutionPlan
            Dependency graph of the terms to be executed.
        dates : pd.DatetimeIndex
            Row labels for our root mask.
        symbols : list
            Column labels for our root mask.
        workspace : dict
            Map from term -> output.
            Must contain at least entry for `self._root_mask_term` whose shape
            is `(len(dates), len(assets))`, but may contain additional
            pre-computed terms for testing or optimization purposes.
        refcounts : dict[Term, int]
            Dictionary mapping terms to number of dependent terms. When a
            term's refcount hits 0, it can be safely discarded from
            ``workspace``. See TermGraph.decref_dependencies for more info.
        execution_order : list[Term]
            Order in which to execute terms.
        hooks : implements(PipelineHooks)
            Hooks to instrument pipeline execution.

        Returns
        -------
        results : dict
            Dictionary mapping requested results to outputs.
        """
        self._validate_compute_chunk_params(graph, dates, symbols, workspace)

        # get_loader = self._get_loader

        # Copy the supplied initial workspace so we don't mutate it in place.
        workspace = workspace.copy()
        domain = graph.domain

        # If loadable terms share the same loader and extra_rows, load them all
        # together. TODO breakpoint
        loader_group_key = juxt(
            lambda x: x.dataset.get_loader(), getitem(
                graph.extra_rows))
        loader_groups = groupby(loader_group_key, graph.loadable_terms)

        for term in execution_order:
            # `term` may have been supplied in `initial_workspace`, or we may
            # have loaded `term` as part of a batch with another term coming
            # from the same loader (see note on loader_group_key above). In
            # either case, we already have the term computed, so don't
            # recompute.
            if term in workspace:
                continue

            # Asset labels are always the same, but date labels vary by how
            # many extra rows are needed.
            mask, mask_dates = graph.mask_and_dates_for_term(
                term,
                self._root_mask_term,
                workspace,
                dates,
            )
            
            if isinstance(term, LoadableTerm):
                # loader = get_loader(term)
                loader = term.dataset.get_loader()
                to_load = sorted(
                    loader_groups[loader_group_key(term)],
                    key=lambda t: t.dataset
                )
                self._ensure_can_load(loader, to_load)
                with hooks.loading_terms(to_load):
                    loaded = loader.load_adjusted_array(
                        to_load, mask_dates, symbols, mask,
                )
                assert set(loaded) == set(to_load), (
                    'loader did not return an AdjustedArray for each column\n'
                    'expected: %r\n'
                    'got:      %r' % (
                        sorted(to_load, key=repr),
                        sorted(loaded, key=repr),
                    )
                )
                workspace.update(loaded)
            else:
                with hooks.computing_term(term):
                    workspace[term] = term._compute(
                        self._inputs_for_term(
                            term,
                            workspace,
                            graph,
                            domain,
                            refcounts,
                        ),
                        mask_dates,
                        symbols,
                        mask,
                    )
                
                if term.ndim == 2:
                    assert workspace[term].shape == mask.shape
                else:
                    assert workspace[term].shape == (mask.shape[0], 1)

                # Decref dependencies of ``term``, and clear any terms
                # whose refcounts hit 0.
                for garbage in graph.decref_dependencies(term, refcounts):
                    del workspace[garbage]

        # At this point, all the output terms are in the workspace.
        out = {}
        graph_extra_rows = graph.extra_rows
        for name, term in iteritems(graph.outputs):
            # Truncate off extra rows from outputs.
            out[name] = workspace[term][graph_extra_rows[term]:]
        return out

    def _to_narrow(self, terms, data, mask, dates, symbols):
        """
        Convert raw computed pipeline results into a DataFrame for public APIs.

        Parameters
        ----------
        terms : dict[str -> Term]
            Dict mapping column names to terms.
        data : dict[str -> ndarray[ndim=2]]
            Dict mapping column names to computed results for those names.
        mask : ndarray[bool, ndim=2]
            Mask array of values to keep.
        dates : ndarray[datetime64, ndim=1]
            Row index for arrays `data` and `mask`
        symbols : list
            Column index

        Returns
        -------
        results : pd.DataFrame
            The indices of `results` are as follows:

            index : two-tiered MultiIndex of (date, asset).
                Contains an entry for each (date, asset) pair corresponding to
                a `True` value in `mask`.
            columns : Index of str
                One column per entry in `data`.

        If mask[date, asset] is True, then result.loc[(date, asset), colname]
        will contain the value of data[colname][date, asset].
        """
        if not mask.any():
            # Manually handle the empty DataFrame case. This is a workaround
            # to pandas failing to tz_localize an empty dataframe with a
            # MultiIndex. It also saves us the work of applying a known-empty
            # mask to each array.
            #
            # Slicing `dates` here to preserve pandas metadata.
            empty_dates = dates[:0]
            empty_assets = array([], dtype=object)
            return DataFrame(
                data={
                    name: array([], dtype=arr.dtype)
                    for name, arr in iteritems(data)
                },
                index=MultiIndex.from_arrays([empty_dates, empty_assets]),
            )


        final_columns = {}
        for name in data:
            # Each term that computed an output has its postprocess method
            # called on the filtered result.
            #
            # As of Mon May 2 15:38:47 2016, we only use this to convert
            # LabelArrays into categoricals.
            final_columns[name] = terms[name].postprocess(data[name][mask])

        # resolved_assets = array(self._finder.retrieve_all(symbols))
        resolved_assets = symbols.to_numpy()
        index = _pipeline_output_index(dates, resolved_assets, mask)

        return DataFrame(data=final_columns, index=index)
    
    def _validate_compute_chunk_params(self,
                                       graph,
                                       dates,
                                       symbols,
                                       initial_workspace):
        """
        Verify that the values passed to compute_chunk are well-formed.
        """
        root = self._root_mask_term
        clsname = type(self).__name__

        # Writing this out explicitly so this errors in testing if we change
        # the name without updating this line.
        compute_chunk_name = self.compute_chunk.__name__
        if root not in initial_workspace:
            raise AssertionError(
                "root_mask values not supplied to {cls}.{method}".format(
                    cls=clsname,
                    method=compute_chunk_name,
                )
            )

        shape = initial_workspace[root].shape
        implied_shape = len(dates), len(symbols)
        if shape != implied_shape:
            raise AssertionError(
                "root_mask shape is {shape}, but received dates/symbols "
                "imply that shape should be {implied}".format(
                    shape=shape,
                    implied=implied_shape,
                )
            )
    def resolve_domain(self, pipeline):
        """Resolve a concrete domain for ``pipeline``.
        """
        domain = pipeline.domain(default=self._default_domain)
        if domain is GENERIC:
            raise ValueError(
                "Unable to determine domain for Pipeline.\n"
                "Pass domain=<desired domain> to your Pipeline to set a "
                "domain."
            )
        return domain

    def _is_special_root_term(self, term):
        return (
            term is self._root_mask_term
            or term is self._root_mask_dates_term
        )

    def _resolve_hooks(self, hooks):
        if hooks is None:
            hooks = []
        return DelegatingHooks(self._default_hooks + hooks)

    def _ensure_can_load(self, loader, terms):
        """Ensure that ``loader`` can load ``terms``.
        """
        if not loader.currency_aware:
            bad = [t for t in terms if t.currency_conversion is not None]
            if bad:
                raise ValueError(
                    "Requested currency conversion is not supported for the "
                    "following terms:\n{}".format(bulleted_list(bad))
                )


def _pipeline_output_index(dates, assets, mask):
    """
    Create a MultiIndex for a pipeline output.

    Parameters
    ----------
    dates : pd.DatetimeIndex
        Row labels for ``mask``.
    assets : pd.Index
        Column labels for ``mask``.
    mask : np.ndarray[bool]
        Mask array indicating date/asset pairs that should be included in
        output index.

    Returns
    -------
    index : pd.MultiIndex
        MultiIndex  containing (date,  asset) pairs  corresponding to  ``True``
        values in ``mask``.
    """
    date_labels = repeat_last_axis(arange(len(dates)), len(assets))[mask]
    asset_labels = repeat_first_axis(arange(len(assets)), len(dates))[mask]
    return MultiIndex(
        levels=[dates, assets],
        #labels=[date_labels, asset_labels],
        codes=[date_labels, asset_labels],
        # TODO: We should probably add names for these.
        names=[None, None],
        verify_integrity=False,
    )