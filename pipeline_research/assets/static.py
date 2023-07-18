import pandas as pd

class StaticAssetFinder:
	"""
	Replaces zipline.assets.AssetFinder

	This class provides methods for looking up assets by by symbol.
	Static means each day all assets are valid.

	Parameters
	----------
	func: list_symbols function to receive symbols using lifetimes function
	
	See Also
	--------
	:class:`pipeline_research.assets.AssetFinder`
	"""

	def __init__(self, list_symbols):
		self._list_symbols = list_symbols

	def lifetimes(self, dates, include_start_date, country_codes):
		"""
		Compute a DataFrame representing asset lifetimes for the specified date
		range.

		In StaticAssetFinder lifetimes produces all True

		Parameters
		----------
		dates : pd.DatetimeIndex
			The dates for which to compute lifetimes.
		include_start_date : bool
			Whether or not to count the asset as alive on its start_date.

			This is useful in a backtesting context where `lifetimes` is being
			used to signify "do I have data for this asset as of the morning of
			this date?"  For many financial metrics, (e.g. daily close), data
			isn't available for an asset until the end of the asset's first
			day.
		country_codes : iterable[str]
			The country codes to get lifetimes for.

		Returns
		-------
		lifetimes : pd.DataFrame
			A frame of dtype bool with `dates` as index and an Int64Index of
			assets as columns.  The value at `lifetimes.loc[date, asset]` will
			be True iff `asset` existed on `date`.  If `include_start_date` is
			False, then lifetimes.loc[date, asset] will be false when date ==
			asset.start_date.

		See Also
		--------
		numpy.putmask
		pipeline_research.engine.SimplePipelineEngine._compute_root_mask
		"""
		symbols = self._list_symbols()
		symbols = [s for s in symbols if isinstance(s, str)]
		symbols = sorted(symbols)

		# All True on all days for static assigment
		df = pd.DataFrame(True, index=dates, columns=symbols)

		return df
