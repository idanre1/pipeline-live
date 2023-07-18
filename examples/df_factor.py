from zipline.pipeline import Pipeline
from pipeline_research.engine import ResearchPipelineEngine
from pipeline_research.assets.static import StaticAssetFinder

from pipeline_research.data.sources.nasdaq import list_symbols
from pipeline_research.data.base_factor import DfFactor

import pandas as pd

# universe is all assets located inside the DataFrame
def list_symbols():
    df = pd.read_parquet('/datadrive/scratch/factor_df.parquet')
    return df.columns.to_list()
assetFinder = StaticAssetFinder(list_symbols)

factor = DfFactor()

eng = ResearchPipelineEngine(assetFinder)
pipe = Pipeline({
    'factor' : factor
})#, screen=top5)

df = eng.run_pipeline(pipe, start_date='2019-01-02', end_date='2020-01-01')
print(df)