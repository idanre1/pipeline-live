import numpy as np

from zipline.pipeline.data.dataset import Column, DataSet
from pipeline_research.data.base_loader import FundamentalLoader

from zipline.utils.numpy_utils import (
    object_dtype, datetime64ns_dtype, datetime64D_dtype, float64_dtype,
)

import algo_data_provider as dp
class Company(DataSet):
    
    df_path = dp.get_nasdaq_sector_data_filename()

    symbol = Column(object_dtype, missing_value='')
    name = Column(object_dtype, missing_value='', metadata={'df_name':'Name'})
    last_sale = Column(float64_dtype, missing_value=np.nan)
    net_change = Column(float64_dtype, missing_value=np.nan)
    pct_change = Column(float64_dtype, missing_value=np.nan)
    mkt_cap = Column(float64_dtype, missing_value=np.nan)
    country = Column(object_dtype, missing_value='')
    ipo_year = Column(float64_dtype, missing_value=np.nan)
    volume = Column(float64_dtype, missing_value=np.nan)
    sector = Column(object_dtype, missing_value='', metadata={'df_name':'Sector'})
    industry = Column(object_dtype, missing_value='')

    _loader = FundamentalLoader(df_path, 'Nasdaq Company Stats')

    @classmethod
    def get_loader(cls):
        return cls._loader

