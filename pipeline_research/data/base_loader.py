import numpy as np
import pandas as pd

from interface import implements

from zipline.pipeline.loaders.base import PipelineLoader
from zipline.utils.numpy_utils import object_dtype

import logging
log = logging.getLogger(__name__)

class EventLoader(implements(PipelineLoader)):

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

