import pandas as pd
import algo_data_provider as dp

def list_symbols():
    df = pd.read_parquet(dp.get_nasdaq_sector_data_filename())
    return df.index.to_list()
