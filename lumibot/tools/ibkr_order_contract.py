"""Bind simulated IBKR orders to tradable contracts, not continuous charts.

A continuous series supplies signals. Once submitted, an order/position keeps
its expiry until the strategy closes it. No implicit roll trades or back-adjusted
execution prices are introduced here.
"""
from lumibot.entities import Asset, Order


def resolve_order_contract(*, asset, side, quantity, positions, when):
    kind = getattr(asset, "asset_type", None)
    if kind != Asset.AssetType.CONT_FUTURE and not (
        kind == Asset.AssetType.FUTURE and asset.expiration is None and asset.auto_expiry
    ):
        return asset
    from lumibot.tools.futures_roll import determine_contract_year_month
    from lumibot.tools.ibkr_helper import _contract_expiration_date

    year, month = determine_contract_year_month(asset.symbol, when)
    expiration = _contract_expiration_date(asset.symbol, year=year, month=month)
    current = Asset(asset.symbol, asset_type=Asset.AssetType.FUTURE,
                    expiration=expiration, multiplier=asset.multiplier,
                    leverage=asset.leverage)
    if getattr(asset, "min_tick", None) is not None:
        current.min_tick = asset.min_tick
    # An explicit opening side must not silently close a different held contract.
    if side in {Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.SELL_TO_OPEN, Order.OrderSide.SELL_SHORT}:
        return current
    buying = side in {Order.OrderSide.BUY, Order.OrderSide.BUY_TO_CLOSE, Order.OrderSide.BUY_TO_COVER}
    opposite = [position for position in positions
                if position.asset.symbol == asset.symbol
                and position.asset.asset_type == Asset.AssetType.FUTURE
                and position.asset.expiration is not None
                and float(position.quantity) * (1 if buying else -1) < 0]
    if len(opposite) > 1:
        raise ValueError(f"Ambiguous {asset.symbol} close across multiple expiries; use position.asset explicitly")
    if opposite:
        held = opposite[0]
        if float(quantity) > abs(float(held.quantity)) and held.asset.expiration != expiration:
            raise ValueError(f"A {asset.symbol} reversal spans different contracts; close position.asset and open the next contract separately")
        return held.asset
    if side in {Order.OrderSide.BUY_TO_CLOSE, Order.OrderSide.BUY_TO_COVER, Order.OrderSide.SELL_TO_CLOSE}:
        raise ValueError(f"No matching held {asset.symbol} contract to close")
    return current
