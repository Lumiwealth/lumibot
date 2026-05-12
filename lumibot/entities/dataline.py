from collections.abc import Sequence
from typing import Any


class Dataline:
    __slots__ = ("asset", "name", "dataline", "dtype")

    def __init__(self, asset: object, name: str, dataline: Sequence[Any], dtype: object) -> None:
        self.asset = asset
        self.name = name
        self.dataline = dataline
        self.dtype = dtype
