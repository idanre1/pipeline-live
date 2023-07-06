from pipeline_research.engine import ResearchPipelineEngine
from zipline.pipeline import Pipeline

from pipeline_research.data.yahoo.pricing import USEquityPricingShifted

def list_symbols():
    return ['MSFT', 'AAPL', 'QCOM', 'AMZN']

open1 = USEquityPricingShifted.open.latest
screen = open1.quantiles(2).eq(1.0)

eng = ResearchPipelineEngine(list_symbols)
pipe = Pipeline({
    'close': USEquityPricingShifted.close.latest,
    'adj_close' : USEquityPricingShifted.adj_close.latest,
    'open' : USEquityPricingShifted.open.latest,
    },
    screen=screen
    )

df = eng.run_pipeline(pipe, start_date='2023-06-01', end_date='2023-06-05')
print(df)