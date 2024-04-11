class AssetFinder:
	"""
	Replaces zipline.assets.AssetFinder

	This class provides methods for looking up assets by by symbol.
	This class resembles to a constituents database as described in https://github.com/fja05680/sp500.git
	
        tickers
    date	
    2007-01-03	[A, AABA, AAPL, ABC, ABI, ABKFQ, ABT, ACS, ADB...
    2007-01-04	[A, AABA, AAPL, ABC, ABI, ABKFQ, ABT, ACS, ADB...

    The DataFrame should be exploded to the following before:
        A	AABA	AAL	AAMRQ	AAP	AAPL	ABBV	ABC	ABI	ABKFQ	...	XRX	XTO	XYL	YNR	YRCW	YUM	ZBH	ZBRA	ZION	ZTS
    date																					
    1996-01-02	False	False	True	True	False	True	False	False	True	False	...	True	False	False	False	True	False	False	False	False	False
    1996-01-03	False	False	True	True	False	True	False	False	True	False	...	True	False	False	False	True	False	False	False	False	False

	Parameters
	----------
	pd.DataFrame: dataframe of constituents of sp500 or any other you like
	
	See Also
	--------
	:class:`pipeline_research.assets.AssetFinder`
	"""

	def __init__(self, constituents):
		self._constituents = constituents.tz_localize('UTC')

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
		# _constituents can be sparse, do date manipulation should arise:
  
		# drop duplicates
		constituents = self._constituents[~self._constituents.index.duplicated(keep='first')]
		# get ffill indexes locations
		ilocs = constituents.index.get_indexer(dates, method='ffill')
		# slice the dataframe and rename indexes to requested dates
		df = constituents.iloc[ilocs]
		df.index = dates
		
		return df
