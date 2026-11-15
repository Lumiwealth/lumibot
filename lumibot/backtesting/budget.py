"""Caller-declared starting-capital conversion, without fetching prices."""

import json
import math
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, InvalidOperation


def _time_relation(value, start):
    """Describe supplied dates without making them a prerequisite for conversion."""
    if value is None or start is None:
        return "unspecified"
    try:
        if isinstance(value, date) and not isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and len(value) == 10:
            parsed = date.fromisoformat(value)
        else:
            parsed = (
                value if isinstance(value, datetime)
                else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            )
        if isinstance(parsed, date) and not isinstance(parsed, datetime):
            return (
                "date_after_start" if parsed > start.date()
                else "date_before_start" if parsed < start.date()
                else "date_at_start"
            )
        if parsed.tzinfo is None or start.tzinfo is None:
            return "unspecified"
        return "after_start" if parsed > start else "before_start" if parsed < start else "at_start"
    except (ValueError, TypeError, AttributeError):
        return "unspecified"


def convert_backtest_budget(amount, quote_asset, conversion, *, environment_value=None, start=None):
    """Return quote-wallet cash and an optional conversion receipt.

    The record's rate is quote-wallet units per input-budget unit. It is supplied
    by the caller, not fetched or independently verified here. Provenance may be
    missing or approximate; known later dates are disclosed rather than blocked.
    """
    supplied_by = "argument"
    if environment_value is not None and environment_value.strip().lower() not in {"", "none", "null"}:
        try:
            conversion = json.loads(environment_value)
        except (ValueError, TypeError) as exc:
            raise ValueError("BACKTESTING_BUDGET_CONVERSION must be a JSON conversion record") from exc
        supplied_by = "environment"
    if conversion is None:
        return amount, None
    if not isinstance(conversion, Mapping):
        raise ValueError("budget_conversion must be a conversion record")
    if amount is None:
        raise ValueError("budget_conversion requires an explicit budget amount")

    currency = conversion.get("budget_currency")
    target = conversion.get("quote_currency")
    if not isinstance(currency, str) or not currency.strip():
        raise ValueError("budget_conversion requires a budget_currency")
    if not isinstance(target, str) or not target.strip():
        raise ValueError("budget_conversion requires a quote currency")
    currency, target = currency.strip().upper(), target.strip().upper()
    if target != str(getattr(quote_asset, "symbol", "")).upper():
        raise ValueError("budget_conversion quote currency must match quote_asset")
    try:
        rate = Decimal(str(conversion.get("quote_per_budget")))
        if not rate.is_finite() or rate <= 0:
            raise ValueError("budget_conversion quote_per_budget must be positive finite")
        initial = Decimal(str(amount))
        converted = initial * rate
        if not initial.is_finite() or not converted.is_finite() or not math.isfinite(float(converted)):
            raise ValueError("converted backtest budget must be finite")
    except InvalidOperation as exc:
        raise ValueError("budget_conversion quote_per_budget must be positive finite") from exc

    receipt = deepcopy(dict(conversion))
    receipt.update(
        input_amount=str(amount),
        budget_currency=currency,
        quote_currency=target,
        quote_per_budget=str(rate),
        quote_amount=str(converted),
        supplied_by=supplied_by,
        as_of=conversion.get("as_of"),
        source=conversion.get("source"),
        note=conversion.get("note"),
        time_relation=_time_relation(conversion.get("as_of"), start),
    )
    return float(converted), receipt
