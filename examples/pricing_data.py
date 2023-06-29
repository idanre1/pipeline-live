from pipeline_research.engine import ResearchPipelineEngine
from zipline.pipeline import Pipeline

from pipeline_research.data.yahoo.pricing import USEquityPricing

def list_symbols():
    return ['MSFT', 'AAPL']

eng = ResearchPipelineEngine(list_symbols)
pipe = Pipeline({
    'close': USEquityPricing.close.latest,
    'adj_close' : USEquityPricing.adj_close.latest,
    'open' : USEquityPricing.open.latest,
})#, screen=top5)

df = eng.run_pipeline(pipe, start_date='2023-06-01', end_date='2023-06-05')
print(df)