from zipline.pipeline.data.dataset import Column, DataSet
from pipeline_research.data.base_loader import USEquityPricingLoader

from zipline.utils.numpy_utils import float64_dtype

class USEquityPricing(DataSet):
    """
    Dataset representing daily trading prices and volumes.
    """
    def get_pandas_df(sids, start, end):
        import yfinance as yf
        # Download prices
        prices = yf.download(list(sids), start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), keepna=True)

        # Rename OHLC columns to a reasonable ones
        prices=prices.stack()
        prices.rename(columns={
            'Adj Close':'adj_close',
            'Close':'close',
            'High':'high',
            'Low':'low',
            'Open':'open',
            'Volume':'volume',
            }, inplace=True)
        return prices.unstack()

    open = Column(float64_dtype)
    high = Column(float64_dtype)
    low = Column(float64_dtype)
    close = Column(float64_dtype)
    adj_close = Column(float64_dtype)
    volume = Column(float64_dtype)

    _loader = USEquityPricingLoader(get_pandas_df, 'Yahoo finance')

    @classmethod
    def get_loader(cls):
        return cls._loader
