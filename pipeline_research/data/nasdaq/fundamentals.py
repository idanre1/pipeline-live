import numpy as np

from zipline.pipeline.data.dataset import Column, DataSet
from pipeline_research.data.base_loader import FundamentalLoader

from zipline.utils.numpy_utils import (
    object_dtype, datetime64ns_dtype, datetime64D_dtype, float64_dtype, int64_dtype
)

class Company(DataSet):
    
    def get_df_path():
        import algo_data_provider as dp
        return dp.get_nasdaq_sector_data_filename()

    symbol = Column(object_dtype, missing_value='', metadata={'df_name':'Symbol'})
    name = Column(object_dtype, missing_value='', metadata={'df_name':'Name'})
    last_sale = Column(float64_dtype, missing_value=np.nan, metadata={'df_name':'Last Sale'})
    net_change = Column(float64_dtype, missing_value=np.nan, metadata={'df_name':'Net Change'})
    pct_change = Column(float64_dtype, missing_value=np.nan, metadata={'df_name':'% Change'})
    mkt_cap = Column(float64_dtype, missing_value=np.nan, metadata={'df_name':'Market Cap'})
    country = Column(object_dtype, missing_value='', metadata={'df_name':'Country'})
    ipo_year = Column(float64_dtype, missing_value=np.nan, metadata={'df_name':'IPO Year'})
    volume = Column(int64_dtype, missing_value=0, metadata={'df_name':'Volume'})
    sector = Column(object_dtype, missing_value='', metadata={'df_name':'Sector'})
    industry = Column(object_dtype, missing_value='', metadata={'df_name':'Industry'})

    _loader = FundamentalLoader(get_df_path(), 'Nasdaq Company Stats')

    @classmethod
    def get_loader(cls):
        return cls._loader

