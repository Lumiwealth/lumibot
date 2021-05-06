class NoDataFound(Exception):
    def __init__(self, source, asset):
        message = (
            f"{source} did not return data for symbol {asset.symbol}. "
            f"Make sure there is no symbol typo or use another data source"
        )
        super(NoDataFound, self).__init__(message)


class UnavailabeTimestep(Exception):
    def __init__(self, source, timestep):
        message = "%s data source does not have data with %r timestep" % (
            source,
            timestep,
        )
        super(UnavailabeTimestep, self).__init__(message)
