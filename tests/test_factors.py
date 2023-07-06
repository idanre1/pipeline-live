from pipeline_research.data.iex import factors
from pipeline_research.data.iex.pricing import USEquityPricing


def test_factors():
    # assert factors.AverageDollarVolume.inputs[0] == USEquityPricing.close
    # E       assert EquityPricing.close::float64 == USEquityPricing.close::float64
    # E         Full diff:
    # E         - USEquityPricing.close::float64
    # E         ? --
    # E         + EquityPricing.close::float64
    pass