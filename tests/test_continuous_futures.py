"""
Tests for continuous futures symbol utilities and Asset helpers.

Strategy-dependent tests are skipped on GitHub CI where no broker/backtesting env exists.
"""

import os
import pytest
from datetime import date, datetime
from unittest.mock import Mock, MagicMock

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools.futures_symbols import (
    parse_contract_symbol,
    symbol_matches_root,
    from_ib_expiration_to_code,
    generate_symbol_variants,
    get_contract_priority_key,
    build_ib_contract_variants,
)


class TestFuturesSymbolsUtilities:
    def test_parse_contract_symbol_tradovate_style(self):
        result = parse_contract_symbol("MNQU5")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_standard_style(self):
        result = parse_contract_symbol("MNQU25")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_dot_notation(self):
        result = parse_contract_symbol("MNQ.U25")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_full_year(self):
        result = parse_contract_symbol("MNQU2025")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_invalid(self):
        assert parse_contract_symbol("INVALID") is None
        assert parse_contract_symbol("") is None
        assert parse_contract_symbol(None) is None
        assert parse_contract_symbol("123") is None

    def test_symbol_matches_root(self):
        assert symbol_matches_root("MNQU5", "MNQ")
        assert symbol_matches_root("MNQU25", "MNQ")
        assert symbol_matches_root("MNQ.U25", "MNQ")
        assert symbol_matches_root("MNQU2025", "MNQ")
        assert symbol_matches_root("MNQ", "MNQ")
        assert not symbol_matches_root("ESU5", "MNQ")
        assert not symbol_matches_root("", "MNQ")
        assert not symbol_matches_root("MNQ", "")

    def test_from_ib_expiration_to_code(self):
        assert from_ib_expiration_to_code(date(2025, 9, 19)) == ("U", "25")
        assert from_ib_expiration_to_code(datetime(2025, 12, 18)) == ("Z", "25")
        assert from_ib_expiration_to_code("202509") == ("U", "25")
        assert from_ib_expiration_to_code(None) is None
        assert from_ib_expiration_to_code("invalid") is None
        assert from_ib_expiration_to_code("20251") is None

    def test_generate_symbol_variants(self):
        variants = generate_symbol_variants("MNQ", "U", "5", "25", "2025")
        assert variants == {"MNQU5", "MNQU25", "MNQ.U25", "MNQU2025"}

    def test_get_contract_priority_key(self):
        priority_list = ["MNQU25", "MNQU5", "MNQZ25", "MNQZ5"]
        assert get_contract_priority_key("MNQU25", priority_list) == 0
        assert get_contract_priority_key("MNQU5", priority_list) == 1
        assert get_contract_priority_key("MNQ.U25", priority_list) == 0
        assert get_contract_priority_key("ESU25", priority_list) == 999999

    def test_build_ib_contract_variants(self):
        variants = build_ib_contract_variants("MNQ", date(2025, 9, 19))
        assert variants == {"MNQU5", "MNQU25", "MNQ.U25", "MNQU2025"}


class TestAssetPotentialContracts:
    def test_includes_single_digit_variants(self):
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        # Find the first quarterly contract (H, M, U, Z) with a two-digit year, e.g., MNQZ25
        quarterly_codes = ["H", "M", "U", "Z"]
        first_quarterly = None
        for c in contracts:
            for q in quarterly_codes:
                # Two-digit year variant pattern, e.g., MNQZ25 or MNQ.Z25
                if f"{q}25" in c:
                    first_quarterly = (q, "25")
                    break
            if first_quarterly:
                break

        assert first_quarterly is not None, f"No quarterly 2-digit variant found in: {contracts}"

        q_code, y2 = first_quarterly
        y1 = y2[-1]
        # Check that the corresponding single-digit variant exists for the same quarter
        single_digit_found = any(f"{q_code}{y1}" in c for c in contracts)
        double_digit_found = any(f"{q_code}{y2}" in c for c in contracts)
        assert single_digit_found, f"Single-digit variant {q_code}{y1} not found in: {contracts}"
        assert double_digit_found, f"Double-digit variant {q_code}{y2} not found in: {contracts}"

    def test_preserves_existing_order(self):
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        assert isinstance(contracts, list)
        assert len(contracts) == len(set(contracts))

    def test_quarterly_contracts_prioritized(self):
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        quarterly_positions = []
        for i, contract in enumerate(contracts):
            if any(month in contract for month in ["H", "M", "U", "Z"]):
                quarterly_positions.append(i)
                if len(quarterly_positions) >= 3:
                    break
        assert len(quarterly_positions) > 0, "No quarterly contracts found"
        assert quarterly_positions[0] < 10, "Quarterly contracts not prioritized"


@pytest.mark.skipif(bool(os.getenv("CI") or os.getenv("GITHUB_ACTIONS")), reason="Strategy tests skipped on CI")
class TestContinuousFuturesPositionMatching:
    """Strategy.get_position() matching for continuous futures; runs locally only."""

    class FakeBacktestingBroker:
        def __init__(self):
            self.name = "fake"
            self.IS_BACKTESTING_BROKER = True
            class _DS:
                SOURCE = "MEMORY"
                datetime_start = None
                datetime_end = None
                _data_store = {}
            self.data_source = _DS()
            class _FilledPositions:
                def __init__(self):
                    self._list = []
                def get_list(self):
                    return self._list
                def __len__(self):
                    return len(self._list)
                def __getitem__(self, idx):
                    return self._list[idx]
                def __setitem__(self, idx, val):
                    self._list[idx] = val
                def append(self, val):
                    self._list.append(val)
            self._filled_positions = _FilledPositions()
            self._add_subscriber = lambda _: None
            self.quote_assets = set()
            # Expose mocks to control inside tests
            self.get_tracked_position = MagicMock()
            self.get_tracked_positions = MagicMock()

    def setup_method(self):
        self.mock_broker = self.FakeBacktestingBroker()
        self.strategy = Strategy(name="TestStrategy", broker=self.mock_broker)
        self.cont_future_asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        self.stock_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)

    def test_non_continuous_future_unchanged(self):
        mock_position = Mock()
        self.mock_broker.get_tracked_position.return_value = mock_position
        result = self.strategy.get_position(self.stock_asset)
        self.mock_broker.get_tracked_position.assert_any_call("TestStrategy", self.stock_asset)
        assert result == mock_position

    def test_continuous_future_exact_match(self):
        mock_position = Mock()
        self.mock_broker.get_tracked_position.return_value = mock_position
        result = self.strategy.get_position(self.cont_future_asset)
        self.mock_broker.get_tracked_position.assert_any_call("TestStrategy", self.cont_future_asset)
        assert result == mock_position

    def test_continuous_future_single_contract_match(self):
        self.mock_broker.get_tracked_position.return_value = None
        tradovate_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock(); mock_position.asset = tradovate_asset
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        result = self.strategy.get_position(self.cont_future_asset)
        assert result == mock_position

    def test_continuous_future_multiple_contracts_priority(self):
        self.mock_broker.get_tracked_position.return_value = None
        sep_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        dec_asset = Asset("MNQZ5", asset_type=Asset.AssetType.FUTURE)
        sep_position = Mock(); sep_position.asset = sep_asset
        dec_position = Mock(); dec_position.asset = dec_asset
        self.mock_broker.get_tracked_positions.return_value = [dec_position, sep_position]
        self.strategy.log_message = Mock()
        result = self.strategy.get_position(self.cont_future_asset)
        assert result in [sep_position, dec_position]
        self.strategy.log_message.assert_called_once()
        log_call = self.strategy.log_message.call_args[0][0]
        assert "Multiple futures contracts found" in log_call
        assert "MNQ" in log_call

    def test_continuous_future_ib_style_positions(self):
        self.mock_broker.get_tracked_position.return_value = None
        ib_asset = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 9, 19))
        mock_position = Mock(); mock_position.asset = ib_asset
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        result = self.strategy.get_position(self.cont_future_asset)
        assert result == mock_position

    def test_continuous_future_no_matches(self):
        self.mock_broker.get_tracked_position.return_value = None
        other_asset = Asset("ESU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock(); mock_position.asset = other_asset
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        result = self.strategy.get_position(self.cont_future_asset)
        assert result is None

    def test_continuous_future_ignores_non_futures(self):
        self.mock_broker.get_tracked_position.return_value = None
        stock_asset = Asset("MNQ", asset_type=Asset.AssetType.STOCK)
        mock_position = Mock(); mock_position.asset = stock_asset
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        result = self.strategy.get_position(self.cont_future_asset)
        assert result is None

    def test_continuous_future_empty_positions(self):
        self.mock_broker.get_tracked_position.return_value = None
        self.mock_broker.get_tracked_positions.return_value = []
        result = self.strategy.get_position(self.cont_future_asset)
        assert result is None
