from uuid import uuid4
from numpy import array
import numpy as np
import pandas as pd
from pandas import DataFrame, MultiIndex
from six import (
    iteritems,
)
from toolz import groupby, juxt
from toolz.curried.operator import getitem

from zipline.lib.adjusted_array import ensure_adjusted_array, ensure_ndarray
from zipline.errors import NoFurtherDataError
from zipline.pipeline.engine import default_populate_initial_workspace
from zipline.pipeline.term import AssetExists, InputDates, LoadableTerm
from zipline.pipeline.domain import US_EQUITIES
from zipline.pipeline.graph import maybe_specialize
from zipline.utils.calendar_utils import get_calendar
from zipline.utils.numpy_utils import (
    as_column,
    repeat_first_axis,
    repeat_last_axis,
)
from zipline.utils.pandas_utils import explode


class ResearchPipelineEngine(object):

    def __init__(self,
                 list_symbols,
                 calendar=None,
                 populate_initial_workspace=None,
                 default_hooks=None,
                 ):
        self._list_symbols = list_symbols
        if calendar is None:
            calendar = get_calendar('NYSE').all_sessions
        self._calendar = calendar

        self._root_mask_term = AssetExists()
        self._root_mask_dates_term = InputDates()

        self._populate_initial_workspace = (
            populate_initial_workspace or default_populate_initial_workspace
        )
        self._default_domain = US_EQUITIES

        if default_hooks is None:
            self._default_hooks = []
        else:
            self._default_hooks = list(default_hooks)
    
    
    def run_pipeline(self, pipeline, start_date, end_date):
        start_date = pd.Timestamp(start_date, tz='UTC')
        end_date = pd.Timestamp(end_date, tz='UTC')
        if end_date < start_date:
            raise ValueError(
                "start_date must be before or equal to end_date \n"
                f"start_date={start_date}, end_date={end_date}"
            )

        domain = US_EQUITIES
        # domain = self.resolve_domain(pipeline)

        graph = pipeline.to_execution_plan(domain,
                                           self._root_mask_term,
                                           start_date,
                                           end_date,
                                           )
        extra_rows = graph.extra_rows[self._root_mask_term]
        root_mask = self._compute_root_mask(domain, start_date, end_date, extra_rows)
        dates, assets, root_mask_values = explode(root_mask)

        initial_workspace = self._populate_initial_workspace(
            {
                self._root_mask_term: root_mask_values,
                self._root_mask_dates_term: as_column(dates.values)
            },
            self._root_mask_term,
            graph,
            dates,
            assets,
        )

        refcounts = graph.initial_refcounts(initial_workspace)
        execution_order = graph.execution_order(initial_workspace, refcounts)

        results = self.compute_chunk(
            graph,
            dates,
            assets,
            initial_workspace,
            refcounts,
            execution_order
        )

        return self._to_narrow(
            graph.outputs,
            results,
            results.pop(graph.screen_name),
            dates[extra_rows:],
            assets,
        )

    def _compute_root_mask(self, domain, start_date, end_date, extra_rows):
        """
        Compute a lifetimes matrix from our AssetFinder, then drop columns that
        didn't exist at all during the query dates.

        Parameters
        ----------
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
        # calendar = domain.sessions()
        calendar = self._calendar

        # if start_date not in calendar:
        #     raise ValueError(
        #         f"Pipeline start date ({start_date}) is not a trading session for "
        #         f"domain {domain}."
        #     )

        # elif end_date not in calendar:
        #     raise ValueError(
        #         f"Pipeline end date {end_date} is not a trading session for "
        #         f"domain {domain}."
        #     )
        
        start_idx, end_idx = calendar.slice_locs(start_date, end_date)
        if start_idx < extra_rows:
            raise NoFurtherDataError.from_lookback_window(
                initial_message="Insufficient data to compute Pipeline:",
                first_date=calendar[0],
                lookback_start=start_date,
                lookback_length=extra_rows,
            )

        # Build lifetimes matrix reaching back to `extra_rows` days before
        # `start_date.`
        symbols = self._list_symbols()
        symbols = [s for s in symbols if isinstance(s, str)]
        dates = calendar[start_idx - extra_rows:end_idx]
        symbols = sorted(symbols)
        lifetimes = pd.DataFrame(True, index=dates, columns=symbols)

        assert lifetimes.index[extra_rows] >= start_date
        assert lifetimes.index[-1] <= end_date


        if not lifetimes.columns.unique:
            columns = lifetimes.columns
            duplicated = columns[columns.duplicated()].unique()
            raise AssertionError("Duplicated sids: %d" % duplicated)

        # Filter out columns that didn't exist between the requested start and
        # end dates.
        existed = lifetimes.iloc[extra_rows:].any()
        ret = lifetimes.loc[:, existed]
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
            # np.ndarray.  Coerce the former to the latter.
            for input_ in specialized:
                input_data = ensure_ndarray(workspace[input_])
                offset = offsets[term, input_]
                input_data = input_data[offset:]
                if refcounts[input_] > 1:
                    input_data = input_data.copy()
                out.append(input_data)
        return out

    def compute_chunk(self, graph, dates, symbols, initial_workspace, refcounts, execution_order):
        """
        Compute the Pipeline terms in the graph for the requested start and end
        dates.

        Parameters
        ----------
        graph : zipline.pipeline.graph.TermGraph
        dates : pd.DatetimeIndex
            Row labels for our root mask.
        symbols : list
            Column labels for our root mask.
        initial_workspace : dict
            Map from term -> output.
            Must contain at least entry for `self._root_mask_term` whose shape
            is `(len(dates), len(symbols))`, but may contain additional
            pre-computed terms for testing or optimization purposes.

        Returns
        -------
        results : dict
            Dictionary mapping requested results to outputs.
        """
        self._validate_compute_chunk_params(dates, symbols, initial_workspace)
        # get_loader = self.get_loader

        # Copy the supplied initial workspace so we don't mutate it in place.
        workspace = initial_workspace.copy()
        domain = graph.domain

        # If loadable terms share the same loader and extra_rows, load them all
        # together.
        loader_group_key = juxt(
            lambda x: x.dataset.get_loader(), getitem(
                graph.extra_rows))
        loader_groups = groupby(loader_group_key, graph.loadable_terms)

        refcounts = graph.initial_refcounts(workspace)

        for term in execution_order:
            # `term` may have been supplied in `initial_workspace`, and in the
            # future we may pre-compute loadable terms coming from the same
            # dataset.  In either case, we will already have an entry for this
            # term, which we shouldn't re-compute.
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
                to_load = sorted(
                    loader_groups[loader_group_key(term)],
                    key=lambda t: t.dataset
                )
                loader = term.dataset.get_loader()
                loaded = loader.load_adjusted_array(
                    to_load, mask_dates, symbols, mask,
                )
                assert set(loaded) == set(to_load), (
                    "loader did not return an AdjustedArray for each column\n"
                    "expected: %r\n"
                    "got:      %r"
                    % (
                        sorted(to_load, key=repr),
                        sorted(loaded, key=repr),
                    )
                )
                workspace.update(loaded)
            else:
                workspace[term] = term._compute(
                    self._inputs_for_term(term, workspace, graph, domain, refcounts),
                    mask_dates,
                    symbols,
                    mask,
                )
                if term.ndim == 2:
                    assert workspace[term].shape == mask.shape
                else:
                    assert workspace[term].shape == (mask.shape[0], 1)

                # Decref dependencies of ``term``, and clear any terms whose
                # refcounts hit 0.
                for garbage_term in graph.decref_dependencies(term, refcounts):
                    del workspace[garbage_term]

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
                    for name, arr in data.items()
                },
                # index=pd.Index(empty_assets),
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
        # resolved_assets = repeat_first_axis(symbols.to_numpy(), len(dates))[mask]
        resolved_assets = symbols.to_numpy()
        index = _pipeline_output_index(dates, resolved_assets, mask)

        return DataFrame(data=final_columns, index=index)
    
    def _validate_compute_chunk_params(
            self, dates, symbols, initial_workspace):
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
    date_labels = repeat_last_axis(np.arange(len(dates)), len(assets))[mask]
    asset_labels = repeat_first_axis(np.arange(len(assets)), len(dates))[mask]
    return MultiIndex(
        levels=[dates, assets],
        #labels=[date_labels, asset_labels],
        codes=[date_labels, asset_labels],
        # TODO: We should probably add names for these.
        names=[None, None],
        verify_integrity=False,
    )