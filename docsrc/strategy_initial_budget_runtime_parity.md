# Strategy `initial_budget` across runtimes

Use `self.initial_budget` for the strategy's starting value. Backtests expose the
configured starting cash. Live strategies expose the first broker-verified portfolio
equity snapshot, including existing positions. Therefore live `initial_budget` and
live cash are not interchangeable.

For long-running strategies, validate the value and persist an accepted baseline in
strategy state when the business rule requires the baseline to survive a process
restart.
