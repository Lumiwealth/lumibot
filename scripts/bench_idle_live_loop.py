"""CPU cost of the idle live-loop tick, for any installed LumiBot version.

The live executor's main loop sleeps 1 s and then runs
``_handle_lifecycle_methods``, which (in the common case) asks the broker for the
market close time twice and the open time once. This script replays exactly those
three broker calls against the real ``Broker`` methods, with the clock frozen at a
moment when the market is open and at one when it is closed, and reports:

* ``tick_ms``: CPU milliseconds per tick
* ``loop_vcpu``: tick_ms / (1000 + tick_ms), the CPU share of a loop that sleeps 1 s
  between ticks

No broker connection, network or credentials are used.
Usage: python scripts/bench_idle_live_loop.py [ticks]
"""
import json
import platform
import sys
import time
from datetime import datetime as real_datetime, timedelta, timezone

import pandas as pd
import pandas_market_calendars as mcal

import lumibot
import lumibot.brokers.broker as broker_module
from lumibot.brokers.broker import Broker

# Tuesday 2026-11-24: 16:00 UTC is inside the NYSE session, 05:00 UTC on the 25th is not.
MOMENTS = {
    "open": real_datetime(2026, 11, 24, 16, 0, tzinfo=timezone.utc),
    "closed": real_datetime(2026, 11, 25, 5, 0, tzinfo=timezone.utc),
}
METHODS = (
    "market_hours",
    "utc_to_local",
    "initialize_market_calendars",
    "_session_close_time",
    "_is_market_open_from_initialized_calendar",
    "market_close_time",
    "market_open_time",
)


def make_stub_class():
    attrs = {"IS_BACKTESTING_BROKER": False, "extended_trading_minutes": 0}
    for name in METHODS:
        if hasattr(Broker, name):
            attrs[name] = getattr(Broker, name)

    def __init__(self, market):
        self.market = market
        self._trading_days = None

    attrs["__init__"] = __init__
    return type("IdleLoopBrokerStub", (), attrs)


def freeze(moment):
    class Frozen(real_datetime):
        @classmethod
        def now(cls, tz=None):
            m = moment.astimezone(tz) if tz is not None else moment.replace(tzinfo=None)
            return cls(m.year, m.month, m.day, m.hour, m.minute, m.second, tzinfo=m.tzinfo)

    broker_module.datetime = Frozen


def tick_ms(market, moment, ticks):
    stub_cls = make_stub_class()
    freeze(moment)
    broker = stub_cls(market)
    if hasattr(broker, "initialize_market_calendars"):
        # The executor initializes the calendar once at startup, about a month wide.
        cal = mcal.get_calendar(market)
        broker.initialize_market_calendars(
            cal.schedule(start_date=moment - timedelta(days=5), end_date=moment + timedelta(days=30))
        )
    broker.market_close_time()
    broker.market_open_time()
    t0 = time.process_time()
    for _ in range(ticks):
        broker.market_close_time()
        broker.market_close_time()
        broker.market_open_time()
    return (time.process_time() - t0) * 1000.0 / ticks


def main():
    ticks = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    real = broker_module.datetime
    out = {
        "lumibot": getattr(lumibot, "__version__", "?"),
        "pandas": pd.__version__,
        "pandas_market_calendars": getattr(mcal, "__version__", "?"),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "ticks": ticks,
        "results": {},
    }
    try:
        with open("/proc/cpuinfo") as fh:
            parts = sorted({line.split(":")[1].strip() for line in fh if line.startswith("CPU part")})
        out["cpu_part"] = parts
    except OSError:
        out["cpu_part"] = platform.processor()
    for market in ("NYSE", "us_futures", "24/7"):
        for label, moment in MOMENTS.items():
            ms = tick_ms(market, moment, ticks)
            out["results"][f"{market}/{label}"] = {
                "tick_ms": round(ms, 3),
                "loop_vcpu": round(ms / (1000.0 + ms), 4),
            }
    broker_module.datetime = real
    # Version-independent yardstick: one fresh NYSE calendar plus a one-week schedule.
    t0 = time.process_time()
    for _ in range(ticks):
        mcal.get_calendar("NYSE").schedule(start_date="2026-11-20", end_date="2026-11-27")
    out["yardstick_fresh_nyse_week_ms"] = round((time.process_time() - t0) * 1000.0 / ticks, 3)
    print("BENCH " + json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
