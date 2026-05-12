from types import SimpleNamespace

from lumibot.strategies.strategy_executor import StrategyExecutor


def _make_executor(data_source):
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = SimpleNamespace(is_backtesting=True)
    executor.broker = SimpleNamespace(data_source=data_source)
    return executor


def test_is_pandas_daily_data_source_only_matches_pure_pandas():
    PandasDataBacktesting = type(
        "PandasDataBacktesting",
        (),
        {"SOURCE": "PANDAS", "_timestep": "day"},
    )
    ThetaDataBacktestingPandas = type(
        "ThetaDataBacktestingPandas",
        (),
        {"SOURCE": "PANDAS", "_timestep": "day"},
    )

    pandas_executor = _make_executor(PandasDataBacktesting())
    assert pandas_executor._is_pandas_daily_data_source() is True

    thetadata_executor = _make_executor(ThetaDataBacktestingPandas())
    assert thetadata_executor._is_pandas_daily_data_source() is False
