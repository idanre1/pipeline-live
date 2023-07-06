from pipeline_research.engine import ResearchPipelineEngine
from zipline.pipeline import Pipeline

from pipeline_research.data.yahoo.pricing import USEquityPricingShifted
from pipeline_research.data.factors.numerical_classifiers import ClassifierToNumeric

def list_symbols():
    return ['MSFT', 'AAPL', 'QCOM', 'AMZN']

opens = USEquityPricingShifted.open.latest
opens_q = opens.quantiles(2) 
opens_n = ClassifierToNumeric(inputs=[opens_q])

screen = opens_q.eq(1.0)

eng = ResearchPipelineEngine(list_symbols)
pipe = Pipeline({
    'close': USEquityPricingShifted.close.latest,
    'adj_close' : USEquityPricingShifted.adj_close.latest,
    'open' : USEquityPricingShifted.open.latest,
    'open_n' : opens_n + opens_n,
    },
    screen=screen
    )

df = eng.run_pipeline(pipe, start_date='2023-06-01', end_date='2023-06-05')
print(df)