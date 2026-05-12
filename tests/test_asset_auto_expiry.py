"""
Unit tests for Asset auto-expiry semantics.

Auto-expiry futures represent a rolling contract selection rule (resolved by the data source/broker),
so `Asset.expiration` remains `None` unless an explicit expiration is provided.
"""

from datetime import date

from lumibot.entities.asset import Asset


class TestAssetAutoExpiry:
    def test_auto_expiry_future_does_not_materialize_expiration(self):
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH,
        )

        assert asset.expiration is None
        assert asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH
        assert "front_month" in repr(asset).lower()

    def test_manual_expiration_overrides_auto_expiry(self):
        manual_expiry = date(2024, 6, 21)
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=manual_expiry,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH,
        )

        assert asset.expiration == manual_expiry
        assert asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH

    def test_auto_expiry_affects_equality_and_hash_when_expiration_is_none(self):
        a1 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        a2 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="next_quarter")

        assert a1 != a2
        assert hash(a1) != hash(a2)

        # When an explicit expiration is provided, the concrete contract is the identity.
        manual_expiry = date(2024, 6, 21)
        b1 = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=manual_expiry, auto_expiry="front_month")
        b2 = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=manual_expiry, auto_expiry="next_quarter")
        assert b1 == b2
        assert hash(b1) == hash(b2)
