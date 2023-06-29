import numpy as np
import pandas as pd

from interface import implements

from zipline.pipeline.loaders.base import PipelineLoader
from zipline.utils.numpy_utils import object_dtype

from zipline.lib.adjusted_array import AdjustedArray
from zipline.pipeline.domain import US_EQUITIES
from zipline.errors import NoFurtherDataError

import logging
log = logging.getLogger(__name__)

class EventLoader(implements(PipelineLoader)):
    """
    PipelineLoader for Event Data
    """
    def _safe_flat_getter(self, symbol, symbols, column):
        data = symbols.get(symbol, None)
        out = column.missing_value
        if data:
            out = data[0].get(column.name, column.missing_value)
        return out


    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # symbols = sids for not raising interface.interface.InvalidSubInterface
        symbol_dict = self._load()
        out = {}
        for c in columns:
            data = np.array([
                self._safe_flat_getter(symbol, symbol_dict, c)
                for symbol in sids
            ], dtype=c.dtype)
            if c.dtype == object_dtype:
                data[data == None] = c.missing_value  # noqa
            out[c] = np.tile(data, (len(dates), 1))
        return out

class FundamentalLoader(implements(PipelineLoader)):
    """
    PipelineLoader for fundamental data from custom Pandas.DataFrame
    """

    def __init__(self, df_path, df_desc):
        self.df_path = df_path
        self.df_desc = df_desc

    def _load(self):
        log.info(f'Loading {self.df_desc}')
        
        # expected dict is symbol as key 
        df = pd.read_parquet(self.df_path)
        return df.T.to_dict()

    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # symbols = sids for not raising interface.interface.InvalidSubInterface
        symbol_dict = self._load()
        out = {}
        for c in columns:
            data = np.array([
                symbol_dict.get(symbol, {}).get(c.metadata['df_name'], c.missing_value)
                for symbol in sids
            ], dtype=c.dtype)
            if c.dtype == object_dtype:
                data[data == None] = c.missing_value  # noqa
            out[c] = np.tile(data, (len(dates), 1))
        return out

class USEquityPricingLoader(implements(PipelineLoader)):
    """
    PipelineLoader for US Equity Pricing data from custom get_pandas_df function
    """

    def __init__(self, get_pandas_df, loader_desc):
        '''
            get_pandas_df(sids, start, end)
            function that loads to Pandas DataFrame multi column index the data
            	Adj Close	    Close	        High	        Low	            Open	        Volume
                AAPL	MSFT	AAPL	MSFT	AAPL	MSFT	AAPL	MSFT	AAPL	MSFT	AAPL	MSFT
                Date												
                2010-01-04 00:00:00-05:00	6.496294	23.572369	7.643214	30.950001	7.660714	31.100000	7.585000	30.590000	7.622500	30.620001	493729600	38409100
                2010-01-05 00:00:00-05:00	6.507525	23.579987	7.656429	30.959999	7.699643	31.100000	7.616071	30.639999	7.664286	30.850000	601904800	49749600
        '''
        self.get_pandas_df = get_pandas_df
        self.loader_desc   = loader_desc

        domain = US_EQUITIES

        self._all_sessions = domain.all_sessions()

    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # load_adjusted_array is called with dates on which the user's algo
        # will be shown data, which means we need to return the data that would
        # be known at the start of each date.  We assume that the latest data
        # known on day N is the data from day (N - 1), so we shift all query
        # dates back by a day.
        start_date, end_date = _shift_dates(
            self._all_sessions, dates[0], dates[-1], shift=1,
        )

        # Domain trading days
        sessions = self._all_sessions
        sessions_shifted = sessions[(sessions >= start_date) & (sessions <= end_date)]
        sessions_non_shifted = sessions[(sessions >= dates[0]) & (sessions <= dates[-1])]

        # fetch prices
        log.info(f'Loading {self.loader_desc}')
        print(f'Loading {self.loader_desc}')
        prices = self.get_pandas_df(sids, start = sessions_shifted[0], end = sessions_non_shifted[-1])
        
        # Build OHLCs symbol by symbol
        dfs = []
        for symbol in sids:
            if symbol not in prices.columns.get_level_values(1):
                df = pd.DataFrame(
                    {c.name: c.missing_value for c in columns},
                    index=sessions_non_shifted
                )
            else:
                df = prices.loc[:,(slice(None),symbol)]
                	#adj_close	Close	high	low	open	volume
                    #SPY	    SPY 	SPY	    SPY	SPY	    SPY
                    #Date						
                    #2010-01-04 00:00:00-05:00	87.791786	113.330002	113.389999	111.510002	112.370003	118944600.0
                    #2010-01-05 00:00:00-05:00	88.024170	113.629997	113.680000	112.849998	113.260002	111579900.0

                df.columns = df.columns.get_level_values(0)

                df = df.reindex(sessions_non_shifted, method='ffill')
            dfs.append(df)

        raw_arrays = {}
        for c in columns:
            colname = c.name
            parsed_values = []
            for df in dfs:
                if not df.empty:
                    value = df[colname].values
                else:
                    value = np.empty(shape=(len(sessions_non_shifted)))
                    value.fill(np.nan)

                parsed_values.append(value)

            raw_arrays[colname] = np.stack(
                parsed_values,
                axis=-1
            )
        out = {}
        for c in columns:
            c_raw = raw_arrays[c.name]
            out[c] = AdjustedArray(
                c_raw.astype(c.dtype),
                {},
                c.missing_value
            )
        return out


def _shift_dates(dates, start_date, end_date, shift):
    try:
        start = dates.get_loc(start_date)
    except KeyError:
        if start_date < dates[0]:
            raise NoFurtherDataError(
                msg=(
                    "Pipeline Query requested data starting on {query_start}, "
                    "but first known date is {calendar_start}"
                ).format(
                    query_start=str(start_date),
                    calendar_start=str(dates[0]),
                )
            )
        else:
            raise ValueError("Query start %s not in calendar" % start_date)

    # Make sure that shifting doesn't push us out of the calendar.
    if start < shift:
        raise NoFurtherDataError(
            msg=(
                "Pipeline Query requested data from {shift}"
                " days before {query_start}, but first known date is only "
                "{start} days earlier."
            ).format(shift=shift, query_start=start_date, start=start),
        )

    try:
        end = dates.get_loc(end_date)
    except KeyError:
        if end_date > dates[-1]:
            raise NoFurtherDataError(
                msg=(
                    "Pipeline Query requesting data up to {query_end}, "
                    "but last known date is {calendar_end}"
                ).format(
                    query_end=end_date,
                    calendar_end=dates[-1],
                )
            )
        else:
            raise ValueError("Query end %s not in calendar" % end_date)
    return dates[start - shift], dates[end - shift]

