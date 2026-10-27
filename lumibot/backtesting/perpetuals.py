"""Collateral and FIFO lots for perpetual backtests, in native contract units."""

from copy import deepcopy
from decimal import Decimal

from lumibot.entities import Asset, Position


def dec(value):
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Perpetual accounting values must be finite")
    return value


class PerpetualAccount:
    def __init__(self, broker):
        self.broker = broker
        self.lots = {}

    @staticmethod
    def key(strategy, instrument):
        return (getattr(strategy, "name", strategy), instrument.asset,
                getattr(instrument, "position_side", None), getattr(instrument, "position_id", None))

    def convert(self, amount, source, target, asset, price):
        amount = dec(amount)
        if source == target or not amount:
            return amount
        spec = self.broker.get_contract_spec(asset)
        price = spec._price(price)
        if (source, target) == (spec.base, spec.quote):
            return amount * price
        if (source, target) == (spec.quote, spec.base):
            return amount / price
        pair = (Asset(source, "crypto"), Asset(target, "crypto"))
        rate = self.broker.data_source.get_last_price(pair[0], quote=pair[1])
        if rate is not None:
            return amount * dec(rate)
        reverse = self.broker.data_source.get_last_price(pair[1], quote=pair[0])
        if reverse is not None:
            return amount / dec(reverse)
        raise ValueError(f"Missing historical currency conversion {source}/{target} for {asset}")

    def wallet(self, strategy, currency):
        if currency == strategy.quote_asset.symbol:
            return dec(strategy.cash)
        position = self.broker.get_tracked_position(strategy.name, Asset(currency, "crypto"))
        return dec(position.quantity) if position is not None else Decimal(0)

    def credit(self, strategy, currency, amount):
        value = self.wallet(strategy, currency) + dec(amount)
        if currency == strategy.quote_asset.symbol:
            strategy._set_cash_position(float(value))
            return
        asset = Asset(currency, "crypto")
        position = self.broker.get_tracked_position(strategy.name, asset)
        if position is None:
            self.broker._filled_positions.append(Position(strategy.name, asset, value))
        else:
            position.quantity = value
            # Replacing the row invalidates position and portfolio caches.
            for index, existing in enumerate(self.broker._filled_positions):
                if existing is position:
                    self.broker._filled_positions[index] = position
                    break

    def unrealized(self, strategy, position, price, currency=None):
        spec = self.broker.get_contract_spec(position.asset)
        lots = self.lots.get(self.key(strategy, position))
        if lots is None:
            # Seeded positions retain an explicit average basis. Never invent a
            # historical entry when no basis was provided.
            entry = position.avg_fill_price
            if entry is None:
                raise ValueError(f"Perpetual starting position needs avg_fill_price: {position.asset}")
            pnl = spec.pnl(position.quantity, entry, price)
        else:
            pnl = sum((spec.pnl(lot["quantity"], lot["price"], price) for lot in lots), Decimal(0))
        return self.convert(pnl, spec.pnl_currency, currency or spec.settle, position.asset, price)

    def reserved(self, strategy, currency):
        return sum((lot["margin"] for key, lots in self.lots.items()
                    if key[0] == strategy.name for lot in lots if lot["settle"] == currency), Decimal(0))

    def sync_position(self, position):
        lots = self.lots.get(self.key(position.strategy, position))
        if lots:
            spec = self.broker.get_contract_spec(position.asset)
            size = sum((abs(lot["quantity"]) for lot in lots), Decimal(0))
            if spec.inverse:
                position.avg_fill_price = float(size / sum((abs(lot["quantity"]) / lot["price"] for lot in lots), Decimal(0)))
            else:
                position.avg_fill_price = float(sum((abs(lot["quantity"]) * lot["price"] for lot in lots), Decimal(0)) / size)
        for index, existing in enumerate(self.broker._filled_positions):
            if existing is position:
                self.broker._filled_positions[index] = position
                break

    def fill(self, strategy, order, price, quantity):
        """Validate a fill before committing collateral and lots; return actual size."""
        asset = order.asset
        spec = self.broker.get_contract_spec(asset)
        key = self.key(strategy, order)
        existing = self.lots.get(key, [])
        position = self.broker._position_for_order(order)
        if not existing and position is not None and position.quantity:
            if position.avg_fill_price is None:
                raise ValueError(f"Perpetual starting position needs avg_fill_price: {asset}")
            existing = [self.lot(asset, spec, position.quantity, position.avg_fill_price,
                                 asset.leverage, "cross", self.broker.data_source.datetime_start)]
        lots = deepcopy(existing)
        held = sum((lot["quantity"] for lot in lots), Decimal(0))
        quantity, price = abs(dec(quantity)), spec._price(price)
        if quantity <= 0:
            raise ValueError("Perpetual fill quantity must be positive")
        signed = quantity if order.is_buy_order() else -quantity
        params = order.custom_params or {}
        reduce = bool(getattr(order, "reduce_only", False) or params.get("reduceOnly", False))
        side = getattr(order, "position_side", None)
        if side in ("long", "short"):
            # Explicit hedge legs can reduce to flat but never cross into the
            # opposite leg. The execution side still controls buy/sell signs.
            reduce = reduce or (side == "long" and signed < 0) or (side == "short" and signed > 0)
        if reduce:
            if not held or held * signed >= 0:
                raise ValueError("Reduce-only perpetual order has no matching position to reduce")
            signed = (1 if signed > 0 else -1) * min(abs(signed), abs(held))
        actual = abs(signed)
        fee_quote = self.broker.calculate_trade_cost(order, strategy, price, quantity=actual)
        fee = self.convert(fee_quote, spec.quote, spec.settle, asset, price)
        remaining, realized = signed, Decimal(0)
        while lots and remaining * lots[0]["quantity"] < 0:
            lot = lots[0]
            take = min(abs(remaining), abs(lot["quantity"]))
            direction = 1 if lot["quantity"] > 0 else -1
            realized += spec.pnl(take * direction, lot["price"], price)
            fraction = take / abs(lot["quantity"])
            lot["margin"] *= 1 - fraction
            lot["quantity"] -= take * direction
            remaining += take * direction
            if not lot["quantity"]:
                lots.pop(0)
        realized = self.convert(realized, spec.pnl_currency, spec.settle, asset, price)
        if remaining:
            leverage = dec(params.get("leverage", asset.leverage))
            if leverage <= 0:
                raise ValueError("Perpetual leverage must be positive")
            mode = params.get("marginMode", "cross")
            lots.append(self.lot(asset, spec, remaining, price, leverage, mode, self.broker.datetime))
            replacement_margin = sum((lot["margin"] for lot in lots), Decimal(0))
            previous_margin = sum((lot["margin"] for lot in self.lots.get(key, [])), Decimal(0))
            available = self.wallet(strategy, spec.settle) + realized - fee - self.reserved(strategy, spec.settle) + previous_margin
            # Cross collateral includes unrealized P&L from other cross lots in
            # this settlement wallet; isolated allocations cannot subsidize it.
            for other_key, other_lots in self.lots.items():
                if other_key[0] != strategy.name or other_key == key:
                    continue
                other_asset = other_key[1]
                mark = self.broker.data_source.get_last_price(other_asset)
                if mark is not None:
                    other_spec = self.broker.get_contract_spec(other_asset)
                    available += sum((self.convert(other_spec.pnl(lot["quantity"], lot["price"], mark),
                                                   other_spec.pnl_currency, spec.settle, other_asset, mark)
                                      for lot in other_lots if lot["settle"] == spec.settle and lot["mode"] == "cross"), Decimal(0))
            available += sum((self.convert(spec.pnl(lot["quantity"], lot["price"], price),
                                           spec.pnl_currency, spec.settle, asset, price)
                              for lot in lots if lot["mode"] == "cross"), Decimal(0))
            if replacement_margin > available:
                raise ValueError(f"Insufficient {spec.settle} collateral for perpetual initial margin")
        # All calculations and conversions succeeded. Margin is reserved, not
        # spent: equity keeps wallet collateral and adds only unrealized P&L.
        self.credit(strategy, spec.settle, realized - fee)
        if lots:
            self.lots[key] = lots
        else:
            self.lots.pop(key, None)
        return actual, fee

    def lot(self, asset, spec, quantity, price, leverage, mode, opened_at):
        exposure = spec.exposure(abs(dec(quantity)), price)["quote"]
        margin = self.convert(exposure / dec(leverage), spec.quote, spec.settle, asset, price)
        return {"quantity": dec(quantity), "price": dec(price), "margin": margin,
                "settle": spec.settle, "mode": mode, "opened_at": opened_at}
