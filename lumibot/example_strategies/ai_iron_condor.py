"""Hybrid iron condor: AI selects contracts once daily; Python manages execution.

The planner cannot trade. Risk monitoring and signed closes continue without
inference, including through the final minutes of the exchange session.
"""

import json
import math
from datetime import date, datetime, timedelta
from types import SimpleNamespace

from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools import get_trading_days


class AIIronCondorStrategy(Strategy):
    parameters = {
        "symbol": "SPY", "stop_at": 0.4, "wing_width": 1.0, "risk_fraction": 0.03,
        "entry_minutes_before_close": 15, "exit_minutes_before_close": 30,
        "close_reprice_minutes": 5, "max_close_attempts": 3, "fee_per_contract": 0.65,
        "model": "chatgpt/gpt-6-luna",
    }

    def initialize(self):
        self.sleeptime = "1M"
        # Entry timing belongs to this strategy; monitoring must not stop at 15:45.
        self.minutes_before_closing = 0
        for name, initial in (("packages", {}), ("entry_day", None), ("ai_usage", []), ("has_entered", False)):
            if getattr(self.vars, name, None) is None:
                setattr(self.vars, name, initial)
        self.agents.create(
            name="planner", model=self.parameters["model"], allow_trading=False,
            reasoning_effort="medium", model_request_timeout_seconds=0, run_timeout_seconds=0,
            system_prompt=(
                "Recommend an iron condor for the requested symbol and next exchange-session expiration. "
                "decision=open means recommend the verified package for Python to execute; it does not place an order. "
                "Return open when the selection checks pass, even though your tools are read-only. Return skip only "
                "when the conditions or available evidence do not support a package. "
                "Check yesterday's VIX close using information available by the current simulation time; "
                "skip if it was above 25 or cannot be established. Select a listed put and call near "
                "0.14 absolute delta with protective options the requested wing width farther out. Verify "
                "all four contracts have available prices at this time. Never use future prices or invent "
                "contracts. Report the four strikes and expiration, or explain why you skip. "
                "FRED VIXCLS is one source for the previous-session VIX close. To keep evidence compact, "
                "request the option chain without all strikes, then inspect strikes for the requested expiration. "
                "The strategy validates sizing and prices and handles every subsequent exit."
            ),
            output_schema={
                "type": "object", "additionalProperties": False,
                "properties": {
                    "decision": {"type": "string", "enum": ["open", "skip"]},
                    "expiration": {"type": "string"}, "reason": {"type": "string"},
                    **{key: {"type": "number"} for key in ("long_put", "short_put", "short_call", "long_call")},
                },
                "required": ["decision", "expiration", "reason", "long_put", "short_put", "short_call", "long_call"],
            },
        )

    def _record(self, action, **facts):
        self.log_message("HYBRID_CONDOR " + json.dumps({
            "time": self.get_datetime().isoformat(), "action": action, **facts}, default=str))

    @staticmethod
    def _leaves(order):
        children = getattr(order, "child_orders", None) or []
        return [leaf for child in children for leaf in AIIronCondorStrategy._leaves(child)] if children else [order]

    def _working_orders(self, assets):
        orders = self.get_orders()
        children = {leaf.identifier for order in orders if getattr(order, "child_orders", None)
                    for leaf in self._leaves(order)}
        return [order for order in orders if order.identifier not in children and order.is_active()
                and any(leaf.asset in assets for leaf in self._leaves(order))]

    @staticmethod
    def _is_condor(positions):
        if len(positions) != 4:
            return False
        puts = sorted((p for p in positions if p.asset.right == "PUT"), key=lambda p: p.asset.strike)
        calls = sorted((p for p in positions if p.asset.right == "CALL"), key=lambda p: p.asset.strike)
        return (len(puts) == len(calls) == 2 and puts[0].quantity > 0 > puts[1].quantity
                and calls[0].quantity < 0 < calls[1].quantity
                and puts[0].asset.strike < puts[1].asset.strike < calls[0].asset.strike < calls[1].asset.strike
                and len({abs(p.quantity) for p in positions}) == 1)

    def _package_price(self, positions, opening=False):
        """Use displayed executable sides, or explicitly labeled historical trade marks."""
        total, bases = 0.0, set()
        unit = math.gcd(*(abs(int(p.quantity)) for p in positions)) if len(positions) > 1 else 1
        for position in positions:
            quote = self.get_quote(position.asset)
            buying = position.quantity > 0 if opening else position.quantity < 0
            bid, ask = getattr(quote, "bid", None), getattr(quote, "ask", None)
            if bid is not None and ask is not None and 0 <= float(bid) <= float(ask):
                price, basis = float(ask if buying else bid), "bid_ask"
            elif self.is_backtesting and getattr(quote, "price", None) is not None:
                price, basis = float(quote.price), "historical_trade_mark_not_nbbo"
            else:
                return None, "missing_price"
            if not math.isfinite(price) or price < 0:
                return None, "invalid_price"
            ratio = abs(float(position.quantity)) / unit if len(positions) > 1 else 1
            total += ratio * (price if buying else -price)
            bases.add(basis)
        return total, "+".join(sorted(bases))

    def _close(self, key, positions, reason):
        state = self.vars.packages.setdefault(key, {})
        state["exit_reason"] = reason  # A rebound does not erase an outstanding required exit.
        active = self._working_orders({p.asset for p in positions})
        if active:
            close_only = all(str(leaf.side).endswith("_to_close") or leaf.identifier in state.get("exit_order_ids", [])
                             for order in active for leaf in self._leaves(order))
            submitted_at = state.get("exit_submitted_at")
            stale = submitted_at and (self.get_datetime() - datetime.fromisoformat(submitted_at)).total_seconds() >= (
                60 * self.parameters["close_reprice_minutes"])
            if not close_only or stale:
                for order in active:
                    if not order.is_canceled():
                        self.cancel_order(order)
                self._record("cancel_before_close", expiration=key, reason=reason)
            return  # Observe actual cancellation/terminal state before another submission.
        if state.get("submit_uncertain"):
            self._record("exit_reconciliation_required", expiration=key)
            return
        if state.get("exit_attempts", 0) >= self.parameters["max_close_attempts"]:
            if not state.get("reported_incomplete"):
                self._record("exit_incomplete", expiration=key, reason=reason)
                state["reported_incomplete"] = True
            return
        price, basis = self._package_price(positions)
        if price is None:
            self._record("exit_unpriced", expiration=key, reason=reason)
            return
        orders = [self.create_order(p.asset, abs(p.quantity),
                                   ("buy_to_close" if p.quantity < 0 else "sell_to_close")
                                   if p.asset.asset_type == "option" else ("buy" if p.quantity < 0 else "sell"),
                                   time_in_force="day", **({"limit_price": round(abs(price), 2)}
                                                          if len(positions) == 1 else {}))
                  for p in positions]
        # Persist before dispatch so an ambiguous outcome cannot cause a blind duplicate.
        state["exit_attempts"] = state.get("exit_attempts", 0) + 1
        state["exit_submitted_at"] = self.get_datetime().isoformat()
        state["exit_order_ids"] = [order.identifier for order in orders]
        try:
            self.submit_order(orders if len(orders) > 1 else orders[0], is_multileg=len(orders) > 1,
                              order_type="debit" if price > 0 else "credit" if price < 0 else "even",
                              price=round(abs(price), 2))
        except Exception:
            state["submit_uncertain"] = True
            raise
        self._record("exit_submitted", expiration=key, reason=reason, price=price, price_basis=basis,
                     quantity=[float(p.quantity) for p in positions])

    def _next_expiration(self):
        today = self.get_datetime().date()
        schedule = get_trading_days("NYSE", start_date=today, end_date=today + timedelta(days=12))
        return next(day.date() for day in schedule.index if day.date() > today)

    def _entry(self):
        today = self.get_datetime().date().isoformat()
        if self.vars.entry_day == today:
            return
        self.vars.entry_day = today
        expiration = self._next_expiration()
        result = self.agents["planner"].run(
            task_prompt="Recommend today's next-session iron condor with decision=open, or explain decision=skip.",
            context={"symbol": self.parameters["symbol"], "expiration": expiration.isoformat(),
                     "as_of": self.get_datetime().isoformat(), "wing_width": self.parameters["wing_width"]},
        )
        usage = {"time": self.get_datetime().isoformat(), "usage": result.usage,
                 "decision": result.parsed.get("decision") if isinstance(result.parsed, dict) else None,
                 "parse_error": result.parse_error, "cache_hit": getattr(result, "cache_hit", False),
                 "completed_requests": [e.payload for e in getattr(result, "events", []) if e.kind == "usage"]}
        self.vars.ai_usage.append(usage)
        self._record("ai_usage", **usage)
        plan = result.parsed
        if not isinstance(plan, dict) or plan.get("decision") != "open":
            self._record("entry_skipped", plan=plan, parse_error=result.parse_error)
            return
        try:
            assets = self._validate_plan(plan, expiration)
        except (ValueError, KeyError, TypeError) as exc:
            self._record("plan_rejected", reason=str(exc))
            return
        # These unit positions express proposed opening sides, not existing holdings.
        legs = [SimpleNamespace(asset=asset, quantity=side) for asset, side in zip(assets, (1, -1, -1, 1))]
        signed_price, basis = self._package_price(legs, opening=True)
        width = max(assets[1].strike - assets[0].strike, assets[3].strike - assets[2].strike)
        if signed_price is None or not 0 < -signed_price < width:
            self._record("entry_unpriced", price=signed_price, price_basis=basis)
            return
        credit = round(-signed_price, 2)
        if not 0 < credit < width:
            self._record("entry_unpriced", price=credit, price_basis=basis)
            return
        multiplier = assets[0].multiplier
        risk_per_unit = (width - credit) * multiplier + 8 * self.parameters["fee_per_contract"]
        quantity = min(math.floor(self.portfolio_value * self.parameters["risk_fraction"] / risk_per_unit),
                       math.floor(self.cash / (width * multiplier + 4 * self.parameters["fee_per_contract"])))
        if quantity < 1:
            self._record("entry_skipped", reason="insufficient risk or cash capacity")
            return
        orders = [self.create_order(asset, quantity, "buy_to_open" if sign > 0 else "sell_to_open",
                                   time_in_force="day") for asset, sign in zip(assets, (1, -1, -1, 1))]
        state = {"entry_credit": credit, "quantity": quantity, "entry_order_ids": [o.identifier for o in orders],
                 "entry_assets": assets, "entry_submit_uncertain": True}
        self.vars.packages[expiration.isoformat()] = state
        self.vars.has_entered = True
        self.submit_order(orders, is_multileg=True, order_type="credit", price=credit)
        state["entry_submit_uncertain"] = False
        self._record("entry_submitted", expiration=expiration, strikes=[a.strike for a in assets],
                     quantity=quantity, credit=credit, price_basis=basis, reason=plan["reason"])

    def _validate_plan(self, plan, expiration):
        if date.fromisoformat(plan["expiration"]) != expiration:
            raise ValueError("expiration must be the next exchange session")
        strikes = [float(plan[key]) for key in ("long_put", "short_put", "short_call", "long_call")]
        if not all(math.isfinite(k) and k > 0 for k in strikes) or strikes != sorted(set(strikes)):
            raise ValueError("four ordered distinct strikes are required")
        wing = self.parameters["wing_width"]
        if not math.isclose(strikes[1] - strikes[0], wing) or not math.isclose(strikes[3] - strikes[2], wing):
            raise ValueError("protective wings must match the configured width")
        chain = self.get_chains(self.parameters["symbol"])
        assets = []
        for strike, right in zip(strikes, ("PUT", "PUT", "CALL", "CALL")):
            if strike not in chain.get("Chains", {}).get(right, {}).get(expiration.isoformat(), []):
                raise ValueError("contract is absent from the observed chain")
            assets.append(Asset(self.parameters["symbol"], asset_type="option", expiration=expiration,
                                strike=strike, right=right, multiplier=chain.get("Multiplier", 100)))
        return assets

    def on_trading_iteration(self):
        groups = {}
        own_positions = [p for p in self.get_positions() if p.quantity
                         and p.asset.symbol == self.parameters["symbol"]]
        for position in own_positions:
            if position.asset.asset_type == "option":
                groups.setdefault(position.asset.expiration.isoformat(), []).append(position)
            elif position.asset.asset_type == "stock" and self.vars.has_entered:
                groups.setdefault("assignment_stock", []).append(position)
        seconds = self.broker.get_time_to_close()
        if seconds is None or seconds <= 0:
            return
        price = self.get_last_price(self.parameters["symbol"])
        for expiration, positions in groups.items():
            state = self.vars.packages.setdefault(expiration, {})
            reason = state.get("exit_reason")
            if not self._is_condor(positions):
                reason = "incomplete_or_unbalanced_package"
            elif date.fromisoformat(expiration) <= self.get_datetime().date() and seconds <= (
                    60 * self.parameters["exit_minutes_before_close"]):
                reason = "expiry_exit"
            elif price is not None and math.isfinite(float(price)) and float(price) > 0:
                shorts = [p.asset.strike for p in positions if p.quantity < 0]
                middle, half_width = (max(shorts) + min(shorts)) / 2, (max(shorts) - min(shorts)) / 2
                if abs(float(price) - middle) >= self.parameters["stop_at"] * half_width:
                    reason = "price_distance_stop"
            if reason:
                self._close(expiration, positions, reason)
        active = self._working_orders({asset for state in self.vars.packages.values()
                                       for asset in state.get("entry_assets", [])})
        uncertain = any(state.get("entry_submit_uncertain") or state.get("submit_uncertain")
                        for state in self.vars.packages.values())
        if not groups and not active and not uncertain and seconds <= 60 * self.parameters["entry_minutes_before_close"]:
            self._entry()


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIIronCondorStrategy.backtest(AlpacaBacktesting)
    else:
        AIIronCondorStrategy().run_live()
