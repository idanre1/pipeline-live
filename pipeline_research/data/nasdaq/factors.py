# https://github.com/pbharrin/alpha-compiler/blob/master/alphacompiler/data/NASDAQ.py
"""
Custom factor to get NASDAQ sector codes from a flat file
downloaded from NASDAQ.  No date information, so if sector
changes the most recent date's sector will be used.

Created by Peter Harrington (pbharrin) on 10/25/17.
"""
from zipline.pipeline.factors import CustomFactor
import algo_data_provider as dp
import pandas as pd
import numpy as np
import os

class Sector(CustomFactor):
    """Returns a value for an SID stored in memory."""
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        df = pd.read_parquet(dp.get_nasdaq_sector_data_filename())
        self.data = df['Sector']
        sector_mapping_dict = {'Basic Materials': 0, 'Consumer Discretionary': 1, 'Consumer Staples': 2, 'Energy': 3, 'Finance': 4, 'Health Care': 5, 'Industrials': 6, 'Miscellaneous': 7, 'Real Estate': 8, 'Technology': 9, 'Telecommunications': 10, 'Utilities': 11}
        self.data = self.data.map(sector_mapping_dict)

    def compute(self, today, assets, out):
        out[:] = self.data[assets].values

if __name__ == '__main__':
    # Single time ingesting of mapping data after downloading a new csv file
    df = pd.read_parquet(dp.get_nasdaq_sector_data_filename())
    # get unique values
    arr = np.unique(df['Sector'].dropna().values)
    # create a map dict
    sector_mapping_dict = {v:i for i,v in list(enumerate(arr))}
    # replace is with sector_mapping_dict
    print(sector_mapping_dict)