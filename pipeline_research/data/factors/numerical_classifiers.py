import numpy as np

from zipline.pipeline.factors import CustomFactor
from zipline.utils.numpy_utils import float64_dtype
from zipline.pipeline.data import Column

class ClassifierToNumeric(CustomFactor):
    """
    calculate quantiles on a Term
    """
    inputs = [Column]
    dtype = float64_dtype
    window_length = 1
    missing_value = -1

    def compute(self, today, assets, out, arrays):
        data = arrays[0]
        out[:] = data.astype(float64_dtype)
