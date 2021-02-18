import logging
import math
from datetime import datetime

import yfinance as yf


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
    sharpe = (ret - risk_free_rate) / vol
    return sharpe


def max_drawdown(_df):
    """Calculate the Max Drawdown, or the biggest percentage drop
    from peak to trough.
    The dataframe _df must include a column "return" that
    has the return for that time period (eg. daily)
    """
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
    romad = ret / mdd["drawdown"]
    return romad


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


def calculate_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    benchmark = yf.Ticker(symbol)
    benchmark_df = benchmark.history(period="max")
    benchmark_df = benchmark_df.loc[
        (benchmark_df.index >= start) & (benchmark_df.index <= end)
    ]
    benchmark_df["pct_change"] = benchmark_df["Close"].pct_change()
    benchmark_df["div_yield"] = benchmark_df["Dividends"] / benchmark_df["Close"]
    benchmark_df["return"] = benchmark_df["pct_change"] + benchmark_df["div_yield"]

    risk_free_rate = get_risk_free_rate()

    performance(benchmark_df, risk_free_rate, symbol)


def get_risk_free_rate():
    # 13 Week Treasury Rate (^IRX)
    risk_free_rate_ticker = yf.Ticker("^IRX")
    risk_free_rate = risk_free_rate_ticker.info["regularMarketPrice"] / 100
    logging.info(f"Risk Free Rate {risk_free_rate*100:0.2f}%")

    return risk_free_rate
