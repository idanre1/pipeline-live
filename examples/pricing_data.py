from zipline.pipeline import Pipeline
from pipeline_research.engine import ResearchPipelineEngine
from pipeline_research.assets.assets import AssetFinder

from pipeline_research.data.yahoo.pricing import USEquityPricing

import pandas as pd

# universe is of a constituents of a virtual etf.
constituents = pd.DataFrame(columns=['MSFT', 'AAPL'])
constituents.loc[pd.Timestamp('2023-06-01')] = dict(MSFT=False, AAPL=True)  # Trading day
constituents.loc[pd.Timestamp('2023-06-02')] = dict(MSFT=True,  AAPL=False) # Trading day
constituents.loc[pd.Timestamp('2023-06-03')] = dict(MSFT=True,  AAPL=False) # NON-Trading day
constituents.loc[pd.Timestamp('2023-06-04')] = dict(MSFT=False, AAPL=True)  # NON-Trading day
constituents.loc[pd.Timestamp('2023-06-05')] = dict(MSFT=True,  AAPL=True)  # Trading day
assetFinder = AssetFinder(constituents)

eng = ResearchPipelineEngine(assetFinder)
pipe = Pipeline({
    'close': USEquityPricing.close.latest,
    'adj_close' : USEquityPricing.adj_close.latest,
    'open' : USEquityPricing.open.latest,
})#, screen=top5)

df = eng.run_pipeline(pipe, start_date='2023-06-01', end_date='2023-06-05')
print(df)