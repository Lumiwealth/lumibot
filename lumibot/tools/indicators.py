import logging
import math
import os
import webbrowser
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import quantstats as qs

# import lumibot.data_sources.alpha_vantage as av
from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities.asset import Asset
from lumibot.tools import to_datetime_aware

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

    Example:
    >>> df = pd.DataFrame({"return": [0.1, 0.2, 0.3, 0.4, 0.5]})
    >>> cagr(df)
    0.3125


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

    drawdown = df["drawdown_pct"].max()
    if math.isnan(drawdown):
        drawdown = 0

    date = df["drawdown_pct"].idxmax()
    if type(date) == float and math.isnan(date):
        date = df.index[0]

    return {"drawdown": drawdown, "date": date}


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
        "total_return": total_return(_df),
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

    print(f"{prefix} CAGR {cagr_adj*100:,.2f}%")
    print(f"{prefix} Volatility {vol_adj*100:,.2f}%")
    print(f"{prefix} Sharpe {sharpe_adj:0.2f}")
    print(
        f"{prefix} Max Drawdown {maxdown_adj['drawdown']*100:,.2f}% on {maxdown_adj['date']:%Y-%m-%d}"
    )
    print(f"{prefix} RoMaD {romad_adj*100:,.2f}%")


def get_symbol_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    # Making start and end datetime aware
    returns_df = yh.get_symbol_data(symbol)
    returns_df = returns_df.loc[
        (returns_df.index.date >= start.date()) & (returns_df.index.date <= end.date())
    ]
    returns_df["pct_change"] = returns_df["Close"].pct_change()
    returns_df["div_yield"] = returns_df["Dividends"] / returns_df["Close"]
    returns_df["return"] = returns_df["pct_change"] + returns_df["div_yield"]

    return returns_df


def calculate_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    start = to_datetime_aware(start)
    end = to_datetime_aware(end)
    benchmark_df = get_symbol_returns(symbol, start, end)

    risk_free_rate = get_risk_free_rate()

    performance(benchmark_df, risk_free_rate, symbol)


def plot_returns(
    df1,
    name1,
    df2,
    name2,
    plot_file_html="backtest_result.html",
    trades_df=None,
    show_plot=True,
):

    dfs_concat = []

    _df1 = df1.copy()
    _df1 = _df1.sort_index(ascending=True)
    _df1.index.name = "datetime"
    _df1[name1] = (1 + _df1["return"]).cumprod()
    _df1[name1][0] = 1
    dfs_concat.append(_df1[name1])

    _df2 = df2.copy()
    _df2 = _df2.sort_index(ascending=True)
    _df2.index.name = "datetime"
    _df2[name2] = (1 + _df2["return"]).cumprod()
    _df2[name2][0] = 1
    dfs_concat.append(_df2[name2])

    df_final = pd.concat(dfs_concat, join="outer", axis=1)

    if trades_df is None or trades_df.empty:
        logging.info("There were no trades in this backtest.")
        return
    else:
        trades_df = trades_df.set_index("time")
        df_final = df_final.merge(
            trades_df, how="outer", left_index=True, right_index=True
        )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[name1],
            mode="lines",
            name=name1,
            connectgaps=True,
            hovertemplate="Value: %{y:$,.2f}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[name2],
            mode="lines",
            name=name2,
            connectgaps=True,
            hovertemplate="Value: %{y:$,.2f}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
        )
    )

    vshift = 0.005

    # Buys
    buys = df_final.copy()
    buys[name1] = buys[name1].fillna(method="bfill")
    buys = buys.loc[df_final["side"] == "buy"]
    buys["plotly_text"] = buys["filled_quantity"].astype(str) + " " + buys["symbol"]
    buys.index.name = "datetime"
    buys = (
        buys.groupby(["datetime", name1])["plotly_text"]
        .apply(lambda x: "<br>".join(x))
        .reset_index()
    )
    buys = buys.set_index("datetime")
    buys["buy_shift"] = buys[name1] * (1 - vshift)

    fig.add_trace(
        go.Scatter(
            x=buys.index,
            y=buys["buy_shift"],
            mode="markers",
            name="buy",
            marker_symbol="triangle-up",
            marker_color="green",
            marker_size=15,
            hovertemplate="Bought<br>%{text}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
            text=buys["plotly_text"],
        )
    )

    # Sells
    sells = df_final.copy()
    sells[name1] = sells[name1].fillna(method="bfill")
    sells = sells.loc[df_final["side"] == "sell"]
    sells["plotly_text"] = sells["filled_quantity"].astype(str) + " " + sells["symbol"]
    sells.index.name = "datetime"
    sells = (
        sells.groupby(["datetime", name1])["plotly_text"]
        .apply(lambda x: "<br>".join(x))
        .reset_index()
    )
    sells = sells.set_index("datetime")
    sells["sell_shift"] = sells[name1] * (1 + vshift)

    fig.add_trace(
        go.Scatter(
            x=sells.index,
            y=sells["sell_shift"],
            mode="markers",
            name="sell",
            marker_color="red",
            marker_size=15,
            marker_symbol="triangle-down",
            hovertemplate="Sold<br>%{text}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
            text=buys["plotly_text"],
        )
    )
    bm_text = f"Compared With {name2}" if name2 else ""
    fig.update_layout(
        title_text=f"{name1} {bm_text}",
        title_font_size=30,
        template="plotly_dark",
    )

    fig.write_html(plot_file_html, auto_open=show_plot)


def create_tearsheet(
    df1,
    strat_name,
    tearsheet_file,
    df2,
    benchmark_asset,
    show_tearsheet,
):
    _df1 = df1.copy()
    if _df1["return"].abs().sum() == 0:
        return None
    _df1["strategy"] = _df1["return"]

    _df2 = df2.copy()
    _df2["benchmark"] = _df2["return"]

    # Uncomment for debugging
    # _df1.to_csv(f"df1-original.csv")
    # _df2.to_csv(f"df2-original.csv")

    df = pd.concat([_df1, _df2], join="outer", axis=1)
    df.index = pd.to_datetime(df.index)
    df["portfolio_value"] = df["portfolio_value"].ffill()
    df["Close"] = df["Close"].ffill()
    df = df.resample("D").ffill()
    df["strategy"] = df["portfolio_value"].pct_change().fillna(0)
    df["benchmark"] = df["Close"].pct_change().fillna(0)
    df = df.loc[:, ["strategy", "benchmark"]]
    df.index = df.index.tz_localize(None)

    # Uncomment for debugging
    # _df1.to_csv(f"df1.csv")
    # _df2.to_csv(f"df2.csv")
    # df.to_csv(f"df-final.csv")

    bm_text = f"Compared to {benchmark_asset}" if benchmark_asset else ""
    title = f"{strat_name} {bm_text}"

    qs.reports.html(
        df["strategy"],
        df["benchmark"],
        title=title,
        output=True,
        download_filename=tearsheet_file,
        # match_dates=True,
    )
    if show_tearsheet:
        url = "file://" + os.path.abspath(str(tearsheet_file))
        webbrowser.open(url)


def get_risk_free_rate():
    try:
        result = yh.get_risk_free_rate()
    except:
        result = 0

    return result
