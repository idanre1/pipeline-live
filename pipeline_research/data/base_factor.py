from zipline.pipeline.factors import CustomFactor

import pandas as pd

class DfFactor(CustomFactor):
    """
    Trun DataFrame into a Factor
    Index = DateTimeIndex
    Columns = Symbols
    """
    inputs = []
    window_length = 1

    def __init__(self, *args, **kwargs):
        df = pd.read_parquet('/datadrive/scratch/factor_df.parquet')
        # Frequency is weekly, but daily is required
        self.data = df.resample('D').ffill()

    def compute(self, today, assets, out):
        out[:] = self.data.loc[today,assets].values
