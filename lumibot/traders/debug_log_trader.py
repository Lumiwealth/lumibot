from collections.abc import Sequence
from typing import Any

from lumibot.traders.trader import Trader


class DebugLogTrader(Trader):
    """I'm just a trader instance with debug turned on by default"""

    def __init__(
        self,
        logfile: object = "",
        backtest: bool = False,
        debug: bool = True,
        strategies: Sequence[Any] | None = None,
        quiet_logs: bool = False,
    ) -> None:
        super().__init__(logfile=logfile, backtest=backtest, debug=debug, strategies=strategies, quiet_logs=quiet_logs)
