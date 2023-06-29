from pipeline_research.engine import ResearchPipelineEngine
from pipeline_research.data.sources.nasdaq import list_symbols
from pipeline_research.data.base_factor import DfFactor
from zipline.pipeline import Pipeline

import pandas as pd
def list_symbols():
    df = pd.read_parquet('/datadrive/scratch/factor_df.parquet')
    return df.columns.to_list()

factor = DfFactor()

eng = ResearchPipelineEngine(list_symbols)
pipe = Pipeline({
    'factor' : factor
})#, screen=top5)

df = eng.run_pipeline(pipe, start_date='2019-01-02', end_date='2020-01-01')
print(df)