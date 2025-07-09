"""
ProjectX Data Source Implementation for Lumibot

Provides market data functionality through ProjectX data feed.
Supports historical data retrieval for futures contracts.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import pandas as pd
from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.projectx_helpers import ProjectXClient
# Import moved to avoid circular dependency
# from lumibot.credentials import PROJECTX_CONFIG


class ProjectXData(DataSource):
    """
    ProjectX data source implementation for market data.

    Provides historical data for futures contracts through ProjectX API.
    Supports multiple underlying brokers through ProjectX gateway.

    Required Configuration:
    - PROJECTX_{FIRM}_API_KEY: API key for the broker
    - PROJECTX_{FIRM}_USERNAME: Username for the broker
    - PROJECTX_{FIRM}_PREFERRED_ACCOUNT_NAME: Account name (recommended)

    Optional Configuration:
    - PROJECTX_FIRM: Explicitly specify firm (auto-detected if not set)
    - PROJECTX_{FIRM}_BASE_URL: Override default API URL
    - PROJECTX_{FIRM}_STREAMING_BASE_URL: Override default streaming URL
    """

    # ProjectX time unit mappings
    TIME_UNIT_MAPPING = {
        "minute": 1,    # Minute bars
        "hour": 2,      # Hourly bars
        "day": 3,       # Daily bars
        "week": 4,      # Weekly bars
        "month": 5,     # Monthly bars
    }

    def __init__(self, config: dict = None, **kwargs):
        """
        Initialize ProjectX data source.

        Args:
            config: Configuration dictionary (optional, auto-detected from environment)
            **kwargs: Additional arguments for parent class
        """
        # Use environment config if not provided
        if config is None:
            from lumibot.credentials import get_projectx_config
            config = get_projectx_config()

        self.config = config
        self.firm = config.get("firm")

        # Validate required configuration
        required_fields = ["api_key", "username", "base_url"]
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            firm_name = config.get("firm", "unknown")
            raise ValueError(
                f"Missing required ProjectX configuration for {firm_name}: {', '.join(missing_fields)}. "
                f"Please set: PROJECTX_{firm_name}_API_KEY, PROJECTX_{firm_name}_USERNAME"
            )

        # Initialize ProjectX client
        self.client = ProjectXClient(config)

        # Setup logging
        self.logger = logging.getLogger(f"ProjectXData_{self.firm}")

        # Contract cache for symbol-to-contract mapping
        self._contract_cache = {}

        # Initialize parent class
        super().__init__(**kwargs)

        self.logger.info(f"ProjectX data source initialized for firm: {self.firm}")

    # ========== Required DataSource Methods ==========

    def get_last_price(self, asset: Asset, quote: Asset = None, 
                      exchange: str = None) -> float:
        """
        Get the last price for an asset.

        Args:
            asset: Asset to get price for
            quote: Quote asset (not used for futures)
            exchange: Exchange name (not used)

        Returns:
            Last price as float, or None if not available
        """
        try:
            # Get recent bars to extract last price
            bars = self.get_bars(
                asset=asset,
                length=1,
                timespan="minute",
                timeshift=0
            )

            if bars is not None and not bars.df.empty:
                return float(bars.df['close'].iloc[-1])

            return None

        except Exception as e:
            self.logger.error(f"Error getting last price for {asset.symbol}: {e}")
            return None

    def get_bars(self, asset: Asset, length: int, timespan: str = "minute", 
                timeshift: int = None, chunk_size: int = None, 
                max_workers: int = None) -> Bars:
        """
        Get historical bars for an asset.

        Args:
            asset: Asset to get bars for
            length: Number of bars to retrieve
            timespan: Time span for bars (minute, hour, day, week, month)
            timeshift: Number of bars to shift back in time
            chunk_size: Not used (for compatibility)
            max_workers: Not used (for compatibility)

        Returns:
            Bars object containing the historical data
        """
        try:
            # Get contract ID for the asset
            contract_id = self._get_contract_id_from_asset(asset)
            if not contract_id:
                self.logger.error(f"Contract not found for asset: {asset.symbol}")
                return None

            # Parse timespan
            unit, unit_number = self._parse_timespan(timespan)
            if unit is None:
                self.logger.error(f"Unsupported timespan: {timespan}")
                return None

            # Calculate date range
            end_datetime = datetime.now()
            if timeshift:
                # Shift back by specified number of periods
                if timespan == "minute":
                    end_datetime -= timedelta(minutes=timeshift)
                elif timespan == "hour":
                    end_datetime -= timedelta(hours=timeshift)
                elif timespan == "day":
                    end_datetime -= timedelta(days=timeshift)
                elif timespan == "week":
                    end_datetime -= timedelta(weeks=timeshift)
                elif timespan == "month":
                    end_datetime -= timedelta(days=timeshift * 30)

            # Calculate start datetime based on length and timespan
            if timespan == "minute":
                start_datetime = end_datetime - timedelta(minutes=length * unit_number)
            elif timespan == "hour":
                start_datetime = end_datetime - timedelta(hours=length * unit_number)
            elif timespan == "day":
                start_datetime = end_datetime - timedelta(days=length * unit_number)
            elif timespan == "week":
                start_datetime = end_datetime - timedelta(weeks=length * unit_number)
            elif timespan == "month":
                start_datetime = end_datetime - timedelta(days=length * unit_number * 30)
            else:
                start_datetime = end_datetime - timedelta(days=length)

            # Retrieve bars from ProjectX
            df = self.client.history_retrieve_bars(
                contract_id=contract_id,
                start_datetime=start_datetime.isoformat(),
                end_datetime=end_datetime.isoformat(),
                unit=unit,
                unit_number=unit_number,
                limit=length + 100,  # Add buffer for filtering
                include_partial_bar=True,
                live=False,
                is_est=True
            )

            if df.empty:
                self.logger.warning(f"No data returned for {asset.symbol}")
                return None

            # Ensure we have the right number of bars
            if len(df) > length:
                df = df.tail(length)

            # Create Bars object
            bars = Bars(
                df=df,
                source="projectx",
                asset=asset,
                raw=df.to_dict()
            )

            self.logger.debug(f"Retrieved {len(df)} bars for {asset.symbol}")
            return bars

        except Exception as e:
            self.logger.error(f"Error getting bars for {asset.symbol}: {e}")
            return None

    def get_yesterday_dividend(self, asset: Asset) -> float:
        """
        Get yesterday's dividend for an asset.

        ProjectX is a futures broker, so dividends are not applicable.
        Returns 0.0 as futures don't have dividends.
        """
        return 0.0

    def get_historical_prices(self, asset: Asset, length: int, timestep: str = "minute", 
                             timeshift=None, quote=None, exchange=None, include_after_hours=True) -> Bars:
        """
        Get historical prices for an asset.

        Args:
            asset: Asset to get prices for
            length: Number of prices to retrieve
            timestep: Time step for prices (minute, hour, day, week, month)
            timeshift: Time shift for historical data
            quote: Quote asset (not used for futures)
            exchange: Exchange (not used)
            include_after_hours: Whether to include after hours data (not used for futures)

        Returns:
            Bars object containing historical data
        """
        try:
            bars = self.get_bars(
                asset=asset,
                length=length,
                timespan=timestep,
                timeshift=timeshift
            )

            return bars

        except Exception as e:
            self.logger.error(f"Error getting historical prices for {asset.symbol}: {e}")
            return None

    def get_chains(self, asset: Asset) -> Dict:
        """
        Get options chains for an asset.

        ProjectX is a futures broker, so options chains are not applicable.
        Raises NotImplementedError as futures don't have options chains.
        """
        raise NotImplementedError("ProjectX is a futures data source - options chains are not supported")

    # ========== Helper Methods ==========

    def _get_contract_id_from_asset(self, asset: Asset) -> str:
        """Get ProjectX contract ID from Lumibot asset."""
        # Check cache first
        cache_key = f"{asset.symbol}_{asset.asset_type}"
        if cache_key in self._contract_cache:
            return self._contract_cache[cache_key]

        try:
            contract_id = None

            # Handle continuous futures using Asset class logic
            if asset.asset_type == Asset.AssetType.CONT_FUTURE:
                self.logger.debug(f"🔄 Resolving continuous future {asset.symbol} using Asset class")

                try:
                    # Use Asset class method to get potential contracts
                    potential_contracts = asset.get_potential_futures_contracts()

                    for contract_symbol in potential_contracts:
                        # Convert to ProjectX format if needed
                        if not contract_symbol.startswith("CON.F.US."):
                            # Parse symbol like "MESU25" -> "CON.F.US.MES.U25"
                            if len(contract_symbol) >= 4:
                                base_symbol = contract_symbol[:-3]  # Remove last 3 chars
                                month_year = contract_symbol[-3:]   # Get month + year code
                                if len(month_year) == 3:
                                    month_code = month_year[0]
                                    year_code = month_year[1:]
                                    contract_id = f"CON.F.US.{base_symbol}.{month_code}{year_code}"
                                else:
                                    contract_id = f"CON.F.US.{asset.symbol}.{month_year}"
                            else:
                                contract_id = f"CON.F.US.{asset.symbol}.U25"  # Fallback
                        else:
                            contract_id = contract_symbol

                        # Use the first potential contract
                        self.logger.debug(f"✅ Using Asset class contract: {contract_id}")
                        break

                except Exception as asset_error:
                    self.logger.warning(f"⚠️ Asset class resolution failed: {asset_error}")

            # Fallback to client method if Asset class didn't work or for other asset types
            if not contract_id:
                contract_id = self.client.find_contract_by_symbol(asset.symbol)

            if contract_id:
                # Cache the result with asset type for better cache key
                self._contract_cache[cache_key] = contract_id
                return contract_id

            self.logger.warning(f"Contract not found for symbol: {asset.symbol}")
            return None

        except Exception as e:
            self.logger.error(f"Error getting contract ID for {asset.symbol}: {e}")
            return None

    def _parse_timespan(self, timespan: str) -> tuple:
        """
        Parse timespan string into ProjectX unit and unit_number.

        Args:
            timespan: Timespan string (e.g., "1minute", "5minute", "1hour", "1day")

        Returns:
            Tuple of (unit, unit_number) or (None, None) if invalid
        """
        try:
            # Handle simple cases first
            if timespan in ["minute", "hour", "day", "week", "month"]:
                unit = self.TIME_UNIT_MAPPING.get(timespan)
                return unit, 1

            # Parse compound timespans like "1minute", "5minute", "1hour", etc.
            import re
            match = re.match(r'(\d+)(\w+)', timespan.lower())

            if match:
                unit_number = int(match.group(1))
                unit_name = match.group(2)

                # Map unit name to ProjectX unit
                unit = self.TIME_UNIT_MAPPING.get(unit_name)
                if unit is not None:
                    return unit, unit_number

            # Handle common aliases
            alias_mapping = {
                "1m": ("minute", 1),
                "5m": ("minute", 5),
                "15m": ("minute", 15),
                "30m": ("minute", 30),
                "1h": ("hour", 1),
                "4h": ("hour", 4),
                "1d": ("day", 1),
                "1w": ("week", 1),
                "1M": ("month", 1),
            }

            if timespan in alias_mapping:
                unit_name, unit_number = alias_mapping[timespan]
                unit = self.TIME_UNIT_MAPPING.get(unit_name)
                return unit, unit_number

            return None, None

        except Exception as e:
            self.logger.error(f"Error parsing timespan {timespan}: {e}")
            return None, None

    def get_contract_details(self, asset: Asset) -> dict:
        """
        Get detailed contract information for an asset.

        Args:
            asset: Asset to get contract details for

        Returns:
            Dictionary containing contract details
        """
        try:
            contract_id = self._get_contract_id_from_asset(asset)
            if not contract_id:
                return {}

            response = self.client.contract_search_id(contract_id)

            if response and response.get("success"):
                return response.get("contract", {})

            return {}

        except Exception as e:
            self.logger.error(f"Error getting contract details for {asset.symbol}: {e}")
            return {}

    def search_contracts(self, search_text: str) -> List[dict]:
        """
        Search for contracts matching the given text.

        Args:
            search_text: Text to search for in contract symbols/names

        Returns:
            List of contract dictionaries
        """
        try:
            response = self.client.contract_search(search_text)

            if response and response.get("success"):
                return response.get("contracts", [])

            return []

        except Exception as e:
            self.logger.error(f"Error searching contracts for '{search_text}': {e}")
            return []

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        """
        Get current quote (bid/ask) for an asset.

        Note: This is a basic implementation using last price.
        Real-time quote data would require streaming connection.

        Args:
            asset: Asset to get quote for
            quote: Quote asset (for cryptocurrency pairs, not used in ProjectX)
            exchange: Exchange to get quote from (not used in ProjectX)

        Returns:
            Quote object with quote information
        """
        try:
            last_price = self.get_last_price(asset)

            if last_price is not None:
                # For futures, we approximate bid/ask with last price
                # In a real implementation, you'd get actual bid/ask from streaming
                spread = last_price * 0.0001  # 0.01% spread approximation

                return Quote(
                    asset=asset,
                    price=last_price,
                    bid=last_price - spread,
                    ask=last_price + spread,
                    timestamp=datetime.now()
                )

            return Quote(asset=asset)

        except Exception as e:
            self.logger.error(f"Error getting quote for {asset.symbol}: {e}")
            return Quote(asset=asset)

    def get_bars_from_datetime(self, asset: Asset, start_datetime: datetime, 
                              end_datetime: datetime, timespan: str = "minute") -> Bars:
        """
        Get historical bars between specific datetime range.

        Args:
            asset: Asset to get bars for
            start_datetime: Start datetime
            end_datetime: End datetime
            timespan: Time span for bars (minute, hour, day, week, month)

        Returns:
            Bars object containing the historical data
        """
        try:
            # Get contract ID for the asset
            contract_id = self._get_contract_id_from_asset(asset)
            if not contract_id:
                self.logger.error(f"Contract not found for asset: {asset.symbol}")
                return None

            # Parse timespan
            unit, unit_number = self._parse_timespan(timespan)
            if unit is None:
                self.logger.error(f"Unsupported timespan: {timespan}")
                return None

            # Retrieve bars from ProjectX
            df = self.client.history_retrieve_bars(
                contract_id=contract_id,
                start_datetime=start_datetime.isoformat(),
                end_datetime=end_datetime.isoformat(),
                unit=unit,
                unit_number=unit_number,
                limit=10000,  # Large limit to get all data in range
                include_partial_bar=True,
                live=False,
                is_est=True
            )

            if df.empty:
                self.logger.warning(f"No data returned for {asset.symbol}")
                return None

            # Create Bars object
            bars = Bars(
                df=df,
                source="projectx",
                asset=asset,
                raw=df.to_dict()
            )

            self.logger.debug(f"Retrieved {len(df)} bars for {asset.symbol} from {start_datetime} to {end_datetime}")
            return bars

        except Exception as e:
            self.logger.error(f"Error getting bars from datetime for {asset.symbol}: {e}")
            return None 
