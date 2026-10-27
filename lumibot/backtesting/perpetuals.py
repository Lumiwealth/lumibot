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
        self.funding_seen = set()
        self.last_event_time = {}
        self.processing_events = False
        broker.backtest_assumptions = []
        broker.perpetual_events = []

    def assumption(self, message):
        if message not in self.broker.backtest_assumptions:
            self.broker.backtest_assumptions.append(message)
            self.broker.logger.warning(message)

    @staticmethod
    def key(strategy, instrument):
        return (getattr(strategy, "name", strategy), instrument.asset,
                getattr(instrument, "position_side", None), getattr(instrument, "position_id", None))

    def convert(self, amount, source, target, asset, price, at=None):
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
        def historical_rate(base, quote):
            data = self.broker.data_source
            if at is None or at == data.get_datetime():
                return data.get_last_price(base, quote=quote)
            timestep = data.get_timestep()
            bars = data.get_historical_prices((base, quote), 2, timestep=timestep,
                                              timeshift=data.get_datetime() - at)
            if bars is None:
                return None
            duration, _ = data.convert_timestep_str_to_timedelta(timestep)
            closed = bars.df[bars.df.index + duration <= at]
            return None if closed.empty else closed.iloc[-1]["close"]
        rate = historical_rate(*pair)
        if rate is not None:
            return amount * dec(rate)
        reverse = historical_rate(pair[1], pair[0])
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
            mode = str(params.get("marginMode", "cross")).lower()
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

    def process_events(self, strategy):
        if self.processing_events:
            return
        keys = [key for key in self.lots if key[0] == strategy.name]
        if not keys:
            return
        self.processing_events = True
        try:
            source, now = self.broker.data_source, self.broker.datetime
            first_open = min(lot["opened_at"] for key in keys for lot in self.lots[key])
            start = self.last_event_time.get(strategy.name, first_open)
            timeline = {}
            for asset in {key[1] for key in keys}:
                try:
                    events = source.get_funding_events(asset, start, now)
                except Exception as exc:
                    self.assumption(f"{asset}: historical funding unavailable ({type(exc).__name__}); omitted")
                    events = None
                if events is None:
                    self.assumption(f"{asset}: historical funding unavailable; funding omitted")
                else:
                    for event in events:
                        timeline.setdefault(event["timestamp"], []).append((asset, event))
                marks = source._perpetual_rows(source._perpetual_input(getattr(source, "mark_prices", None), asset)) or []
                for row in marks:
                    if start < row["timestamp"] <= now:
                        timeline.setdefault(row["timestamp"], [])
            timeline.setdefault(now, [])
            for at, events in sorted(timeline.items()):
                for asset, event in events:
                    identity = (strategy.name, asset, event["timestamp"])
                    if identity in self.funding_seen:
                        continue
                    spec = self.broker.get_contract_spec(asset)
                    mark = event.get("markPrice")
                    if mark is None:
                        mark, exact = source.get_perpetual_mark(asset, at)
                        if not exact:
                            self.assumption(f"{asset}: funding uses causal candle prices where historical mark data is unavailable")
                    if mark is None:
                        self.assumption(f"{asset}: funding event without an available historical price omitted")
                        self.funding_seen.add(identity)
                        continue
                    for key in list(self.lots):
                        if key[0] != strategy.name or key[1] != asset:
                            continue
                        amount = Decimal(0)
                        for lot in self.lots[key]:
                            if lot["opened_at"] >= at:
                                continue
                            exposure = spec.exposure(lot["quantity"], mark)["quote"]
                            payment = self.convert(-exposure * dec(event["fundingRate"]), spec.quote, spec.settle, asset, mark, at=at)
                            amount += payment
                            if lot["mode"] == "isolated":
                                lot["margin"] += payment
                        if amount:
                            self.credit(strategy, spec.settle, amount)
                            self.broker.perpetual_events.append({"type": "funding", "strategy": strategy.name,
                                "symbol": asset.symbol, "timestamp": at.isoformat(), "amount": str(amount),
                                "currency": spec.settle, "position_side": key[2], "position_id": key[3]})
                    self.funding_seen.add(identity)
                self.liquidate(strategy, at)
            self.last_event_time[strategy.name] = now
        finally:
            self.processing_events = False

    def liquidate(self, strategy, at):
        source = self.broker.data_source
        groups, close = {}, []
        for key, lots in list(self.lots.items()):
            if key[0] != strategy.name:
                continue
            asset = key[1]
            spec = self.broker.get_contract_spec(asset)
            mark, exact = source.get_perpetual_mark(asset, at)
            if not exact:
                self.assumption(f"{asset}: liquidation uses causal candle marks; intrabar liquidation is not modeled")
            if mark is None:
                continue
            risk = source.get_perpetual_risk(asset)
            if risk is None:
                self.assumption(f"{asset}: maintenance tiers and liquidation fees unavailable; liquidation uses collateral exhaustion")
                risk = {}
            maintenance_rate = dec(risk.get("maintenance_margin_rate", 0))
            quantity = sum((lot["quantity"] for lot in lots), Decimal(0))
            pnl = sum((self.convert(spec.pnl(lot["quantity"], lot["price"], mark), spec.pnl_currency,
                                    spec.settle, asset, mark, at=at) for lot in lots), Decimal(0))
            margin = sum((lot["margin"] for lot in lots), Decimal(0))
            maintenance = self.convert(abs(spec.exposure(quantity, mark)["quote"]) * maintenance_rate,
                                       spec.quote, spec.settle, asset, mark, at=at)
            item = (key, mark, quantity, risk)
            if lots[0]["mode"] == "isolated":
                if margin + pnl <= maintenance:
                    close.append(item)
            else:
                group = groups.setdefault(spec.settle, {"pnl": Decimal(0), "maintenance": Decimal(0), "items": []})
                group["pnl"] += pnl
                group["maintenance"] += maintenance
                group["items"].append(item)
        for currency, group in groups.items():
            isolated = sum((lot["margin"] for key, lots in self.lots.items() if key[0] == strategy.name
                            for lot in lots if lot["settle"] == currency and lot["mode"] == "isolated"), Decimal(0))
            if self.wallet(strategy, currency) - isolated + group["pnl"] <= group["maintenance"]:
                close.extend(group["items"])
        for key, mark, quantity, risk in close:
            if key not in self.lots:
                continue
            asset = key[1]
            spec = self.broker.get_contract_spec(asset)
            isolated = all(lot["mode"] == "isolated" for lot in self.lots[key])
            allocation = sum((lot["margin"] for lot in self.lots[key]), Decimal(0))
            wallet_before = self.wallet(strategy, spec.settle)
            order = strategy.create_order(asset, abs(quantity), "sell" if quantity > 0 else "buy",
                position_side=key[2], position_id=key[3], reduce_only=True, custom_params={"liquidation": True})
            self.broker._process_new_order(order)
            self.broker._execute_filled_order(order, float(mark), abs(quantity), strategy)
            fee = self.convert(abs(spec.exposure(quantity, mark)["quote"]) * dec(risk.get("liquidation_fee_rate", 0)),
                               spec.quote, spec.settle, asset, mark)
            insurance_credit = Decimal(0)
            if isolated:
                insurance_credit = max(Decimal(0), wallet_before - allocation - (self.wallet(strategy, spec.settle) - fee))
                if insurance_credit:
                    self.assumption(f"{asset}: isolated gap loss beyond allocated collateral uses modeled insurance credit")
            self.credit(strategy, spec.settle, insurance_credit - fee)
            self.broker.perpetual_events.append({"type": "liquidation", "strategy": strategy.name,
                "symbol": asset.symbol, "timestamp": at.isoformat(), "price": str(mark),
                "quantity": str(quantity), "fee": str(fee), "currency": spec.settle,
                "position_side": key[2], "position_id": key[3], "insurance_credit": str(insurance_credit)})
