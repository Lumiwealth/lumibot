class Dataline:
    __slots__ = ("asset", "name", "dataline", "dtype")

    def __init__(self, asset, name, dataline, dtype):
        self.asset = asset
        self.name = name
        self.dataline = dataline
        self.dtype = dtype
