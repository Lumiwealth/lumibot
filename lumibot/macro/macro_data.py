import os
from typing import Any

from .fred import FREDMacroData
from .fxmacrodata import FXMacroData


class MacroData(FREDMacroData):
    """Strategy macro-data container.

    ``MacroData`` preserves the historical ``self.macro`` FRED methods while
    exposing additional providers under named attributes.
    """

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        api_key: str | None = None,
        min_request_interval_seconds: float = 0.2,
        fxmacrodata: FXMacroData | None = None,
        fxmacrodata_api_key: str | None = None,
        fxmacrodata_base_url: str | None = None,
        fxmacrodata_cache_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        super().__init__(
            strategy,
            cache_dir=cache_dir,
            api_key=api_key,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        self.fred = self
        self.fxmacrodata = fxmacrodata or FXMacroData(
            strategy,
            api_key=fxmacrodata_api_key,
            base_url=fxmacrodata_base_url,
            cache_dir=fxmacrodata_cache_dir,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        self.fxmd = self.fxmacrodata
