import time
from typing import Optional, Dict, Any

import pandas as pd
import pytz

from lumibot.entities import Asset, Bars
from lumibot.data_sources.data_source import DataSource
from lumibot.tools.bitunix_helpers import BitUnixClient

class BitunixData(DataSource):
    SOURCE = "BITUNIX"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1", "1m", "minute"]},
        {"timestep": "3 minutes", "representations": ["3", "3m"]},
        {"timestep": "5 minutes", "representations": ["5", "5m"]},
        {"timestep": "15 minutes", "representations": ["15", "15m"]},
        {"timestep": "30 minutes", "representations": ["30", "30m"]},
        {"timestep": "hour", "representations": ["60", "60m", "1h", "hour"]},
        {"timestep": "2 hours", "representations": ["120", "120m", "2h"]},
        {"timestep": "4 hours", "representations": ["240", "240m", "4h"]},
        {"timestep": "day", "representations": ["D", "1d", "day"]},
    ]

    def __init__(self, config: dict, max_workers: int = 1, chunk_size: int = 100, tzinfo: Optional[pytz.timezone] = None):
        super().__init__(delay=0, tzinfo=tzinfo)
        # Ensure we have a timezone
        if self.tzinfo is None:
            self.tzinfo = pytz.utc
        self.name = "bitunix"
        self.chunk_size = chunk_size
        # Parse API keys
        if isinstance(config, dict):
            try:
                self.api_key = config["API_KEY"]
                self.api_secret = config["API_SECRET"]
            except KeyError:
                raise ValueError("API_KEY and API_SECRET must be provided in config")
        else:
            self.api_key = getattr(config, "API_KEY", None)
            self.api_secret = getattr(config, "API_SECRET", None)
            if not self.api_key or not self.api_secret:
                raise ValueError("API_KEY and API_SECRET must be provided in config")
        self.client = BitUnixClient(self.api_key, self.api_secret)
        # Track symbols we're interested in for WebSocket subscriptions
        self.client_symbols = set()

    def _sanitize_base_and_quote_asset(self, base_asset, quote_asset) -> tuple[Asset, Asset]:
        """Ensure base and quote are Asset and set defaults for spot/futures."""
        if isinstance(base_asset, tuple):
            asset, quote = base_asset
        else:
            asset, quote = base_asset, quote_asset

        if not isinstance(asset, Asset):
            asset = Asset(symbol=str(asset), asset_type=Asset.AssetType.CRYPTO)
        if quote and not isinstance(quote, Asset):
            quote = Asset(symbol=str(quote), asset_type=Asset.AssetType.CRYPTO)

        if asset.asset_type == Asset.AssetType.FUTURE:
            # futures do not need explicit quote asset
            quote = None
        elif asset.asset_type == Asset.AssetType.CRYPTO and quote is None:
            # default spot quote
            quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
        return asset, quote

    def get_last_price(self, asset: Asset, quote: Asset = Asset("USDT", Asset.AssetType.CRYPTO), **kwargs) -> Optional[float]:
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if asset.asset_type == Asset.AssetType.FUTURE:
            symbol = asset.symbol
        else:
            symbol = f"{asset.symbol}USDT"
        
        # For futures, use mark price
        try:
            resp = self.client.get_funding_rate(symbol)
            if resp and resp.get("code") == 0:
                price_str = resp.get("data", {}).get("markPrice")
                return float(price_str) if price_str else None
        except Exception as e:
            print(e)
            return None
    
        return None
    
    def _parse_source_timestep(self, timestep: str) -> str:
        """Convert Lumibot timestep to BitUnix interval format."""
        normalized = self.get_timestep_from_string(timestep)
        
        if normalized == "minute":
            return "1m"
        elif normalized == "3 minutes":
            return "3m"
        elif normalized == "5 minutes":
            return "5m"
        elif normalized == "15 minutes":
            return "15m"
        elif normalized == "30 minutes":
            return "30m"
        elif normalized == "hour":
            return "1h"
        elif normalized == "2 hours":
            return "2h"
        elif normalized == "4 hours":
            return "4h"
        elif normalized == "day":
            return "1d"
        else:
            # Default to 1m if unknown
            return "1m"

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "",
        timeshift=None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True
    ) -> Optional[Bars]:
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if not timestep:
            timestep = self.get_timestep()
            
        # Determine symbol format based on asset type
        if asset.asset_type == Asset.AssetType.FUTURE:
            symbol = asset.symbol
        else:
            symbol = f"{asset.symbol}{quote.symbol}"
            
        # Add to tracked symbols
        self.client_symbols.add(symbol)
        
        # Convert Lumibot timestep to BitUnix interval format
        interval = self._parse_source_timestep(timestep)
        
        try:
            # Calculate limit - request more than needed to ensure we get enough data
            limit = min(1000, length * 2)  # BitUnix might limit to 1000 candles
            
            resp = self.client.get_kline(symbol=symbol, interval=interval, limit=limit)
            if resp and resp.get("code") == 0:
                bars_data = resp.get("data", [])
                if not bars_data:
                    return None
                
                # Construct DataFrame from candle data
                df = pd.DataFrame(bars_data)
                
                # Expected format from documentation - adjust if needed
                if "t" in df.columns:  # Timestamp
                    df["ts"] = df["t"]
                elif "time" in df.columns:  # Also handle 'time' column
                    df["ts"] = df["time"]
                if "o" in df.columns:  # Open
                    df["open"] = df["o"]
                if "h" in df.columns:  # High
                    df["high"] = df["h"]
                if "l" in df.columns:  # Low
                    df["low"] = df["l"]
                if "c" in df.columns:  # Close
                    df["close"] = df["c"]
                if "baseVol" in df.columns:  # Volume
                    df["volume"] = df["baseVol"]
                
                # Ensure numeric columns
                for col in ("open", "high", "low", "close", "volume"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                
                # Set timestamp as index
                if "ts" in df.columns:
                    df.index = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms")
                    # Convert timezone
                    df.index = df.index.tz_localize(pytz.utc).tz_convert(self.tzinfo)
                
                # Select only required columns
                required_cols = ["open", "high", "low", "close", "volume"]
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = 0.0
                
                # Limit to the requested length
                df = df.sort_index()
                if len(df) > length:
                    df = df.tail(length)
                
                # Wrap in Bars object
                return self._parse_source_symbol_bars(
                    df[required_cols], 
                    asset, 
                    quote=None if asset.asset_type == Asset.AssetType.FUTURE else quote, 
                    length=length
                )
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None


    def _parse_source_symbol_bars(self, df: pd.DataFrame, asset: Asset, quote: Asset = None, length: int = None) -> Bars:
        """
        Wraps the raw DataFrame into a Bars entity with source metadata.
        """
        return Bars(df, self.SOURCE, asset, raw=df, quote=quote)

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 100) -> dict:
        """Option chains not supported by BitUnix."""
        return {"Multiplier": 1, "Exchange": exchange or "", "Chains": {}}

    def get_timestep_from_string(self, timestep: str) -> str:
        """
        Maps a string representation of a timestep to the normalized timestep.
        """
        ts = timestep.lower().strip()
        for mapping in self.TIMESTEP_MAPPING:
            if ts in [r.lower() for r in mapping["representations"]]:
                return mapping["timestep"]
        # Default to "minute" if not found
        return "minute"
