from pipeline_research.engine import ResearchPipelineEngine
from pipeline_research.data.sources.nasdaq import list_symbols
from pipeline_research.data.nasdaq.factors import Sector
from zipline.pipeline import Pipeline

eng = ResearchPipelineEngine(list_symbols)
pipe = Pipeline({
    'sector': Sector(),
})#, screen=top5)

df = eng.run_pipeline(pipe, start_date='2019-01-02', end_date='2020-01-01')
print(df)