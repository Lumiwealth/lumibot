import logging
import math
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import pandas as pd

# import lumibot.data_sources.alpha_vantage as av
from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities.asset import Asset

from .yahoo_helper import YahooHelper as yh


def total_return(_df):
    """Calculate the cumulative return in a dataframe
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    df = _df.copy()
    df = df.sort_index(ascending=True)
    df["cum_return"] = (1 + df["return"]).cumprod()

    total_ret = df["cum_return"][-1] - 1

    return total_ret


def cagr(_df):
    """Calculate the Compound Annual Growth Rate
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    df = _df.copy()
    df = df.sort_index(ascending=True)
    df["cum_return"] = (1 + df["return"]).cumprod()
    total_ret = df["cum_return"][-1]
    start = datetime.utcfromtimestamp(df.index.values[0].astype("O") / 1e9)
    end = datetime.utcfromtimestamp(df.index.values[-1].astype("O") / 1e9)
    period_years = (end - start).days / 365.25
    if period_years == 0:
        return 0
    CAGR = (total_ret) ** (1 / period_years) - 1
    return CAGR


def volatility(_df):
    """Calculate the volatility (standard deviation)
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    df = _df.copy()
    start = datetime.utcfromtimestamp(df.index.values[0].astype("O") / 1e9)
    end = datetime.utcfromtimestamp(df.index.values[-1].astype("O") / 1e9)
    period_years = (end - start).days / 365.25
    if period_years == 0:
        return 0
    ratio_to_annual = df["return"].count() / period_years
    vol = df["return"].std() * math.sqrt(ratio_to_annual)
    return vol


def sharpe(_df, risk_free_rate):
    """Calculate the Sharpe Rate, or (CAGR - risk_free_rate) / volatility
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily).
    risk_free_rate should be either LIBOR, or the shortest possible US Treasury Rate
    """
    ret = cagr(_df)
    vol = volatility(_df)
    if vol == 0:
        return 0
    sharpe = (ret - risk_free_rate) / vol
    return sharpe


def max_drawdown(_df):
    """Calculate the Max Drawdown, or the biggest percentage drop
    from peak to trough.
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    if _df.shape[0] == 1:
        return {"drawdown": 0, "date": _df.index[0]}
    df = _df.copy()
    df = df.sort_index(ascending=True)
    df["cum_return"] = (1 + df["return"]).cumprod()
    df["cum_return_max"] = df["cum_return"].cummax()
    df["drawdown"] = df["cum_return_max"] - df["cum_return"]
    df["drawdown_pct"] = df["drawdown"] / df["cum_return_max"]

    max_dd = df.loc[df["drawdown_pct"].idxmax()]

    return {"drawdown": max_dd["drawdown_pct"], "date": max_dd.name}


def romad(_df):
    """Calculate the Return Over Maximum Drawdown (RoMaD)
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    ret = cagr(_df)
    mdd = max_drawdown(_df)
    if mdd["drawdown"] == 0:
        return 0
    romad = ret / mdd["drawdown"]
    return romad


def stats_summary(_df, risk_free_rate):
    return {
        "cagr": cagr(_df),
        "volatility": volatility(_df),
        "sharpe": sharpe(_df, risk_free_rate),
        "max_drawdown": max_drawdown(_df),
        "romad": romad(_df),
    }


def performance(_df, risk_free, prefix=""):
    """Calculate and print out all of our performance indicators
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
    cagr_adj = cagr(_df)
    vol_adj = volatility(_df)
    sharpe_adj = sharpe(_df, risk_free)
    maxdown_adj = max_drawdown(_df)
    romad_adj = romad(_df)

    print(f"{prefix} CAGR {cagr_adj*100:0.2f}%")
    print(f"{prefix} Volatility {vol_adj*100:0.2f}%")
    print(f"{prefix} Sharpe {sharpe_adj:0.2f}")
    print(
        f"{prefix} Max Drawdown {maxdown_adj['drawdown']*100:0.2f}% on {maxdown_adj['date']:%Y-%m-%d}"
    )
    print(f"{prefix} RoMaD {romad_adj*100:0.2f}%")


def get_symbol_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    # Making start and end datetime aware
    start = LUMIBOT_DEFAULT_PYTZ.localize(start, is_dst=None)
    end = LUMIBOT_DEFAULT_PYTZ.localize(end, is_dst=None)

    returns_df = yh.get_symbol_data(symbol)
    returns_df = returns_df.loc[(returns_df.index >= start) & (returns_df.index <= end)]
    returns_df["pct_change"] = returns_df["Close"].pct_change()
    returns_df["div_yield"] = returns_df["Dividends"] / returns_df["Close"]
    returns_df["return"] = returns_df["pct_change"] + returns_df["div_yield"]

    return returns_df


def calculate_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    benchmark_df = get_symbol_returns(symbol, start, end)

    risk_free_rate = get_risk_free_rate()

    performance(benchmark_df, risk_free_rate, symbol)


def plot_returns(df1, name1, df2, name2, plot_file="backtest_result.pdf"):
    dfs_concat = []

    _df1 = df1.copy()
    _df1 = _df1.sort_index(ascending=True)
    _df1[name1] = (1 + _df1["return"]).cumprod()
    _df1.index = _df1.index.date
    dfs_concat.append(_df1.loc[:, [name1]])

    _df2 = df2.copy()
    _df2 = _df2.sort_index(ascending=True)
    _df2[name2] = (1 + _df2["return"]).cumprod()
    _df2.index = _df2.index.date
    dfs_concat.append(_df2.loc[:, [name2]])

    df_final = pd.concat(dfs_concat, join="outer", axis=1)
    df_final.plot()
    plt.savefig(plot_file)


def get_risk_free_rate():
    result = None
    try:
        result = yh.get_risk_free_rate()
    except:
        pass

    return result
