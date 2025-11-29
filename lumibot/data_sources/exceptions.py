class NoDataFound(Exception):
    def __init__(self, source, asset):
        message = (
            f"{source} did not return data for symbol {asset}. "
            f"Make sure there is no symbol typo or use another data source"
        )
        super().__init__(message)


class UnavailabeTimestep(Exception):
    def __init__(self, source, timestep):
        message = f"{source} data source does not have data with {timestep!r} timestep"
        super().__init__(message)
