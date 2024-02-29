import contextlib
import logging
import math
import os
import webbrowser
from datetime import datetime
from decimal import Decimal

import pandas as pd
import plotly.graph_objects as go
import quantstats_lumi as qs
from plotly.subplots import make_subplots

from lumibot import LUMIBOT_DEFAULT_TIMEZONE
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

    total_ret = df["cum_return"].iloc[-1] - 1

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
    total_ret = df["cum_return"].iloc[-1]
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
    print(f"{prefix} Max Drawdown {maxdown_adj['drawdown']*100:,.2f}% on {maxdown_adj['date']:%Y-%m-%d}")
    print(f"{prefix} RoMaD {romad_adj*100:,.2f}%")


def get_symbol_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    """Get the returns for a symbol between two dates

    Parameters
    ----------
    symbol : str
        The symbol to get the returns for
    start : datetime, optional
        The start date, by default datetime(1900, 1, 1)
    end : datetime, optional
        The end date, by default datetime.now()

    Returns
    -------
    pd.DataFrame
        A dataframe with the returns for the symbol. Includes the columns:
        - pct_change: The percent change in the Close price
        - div_yield: The dividend yield
        - return: The pct_change + div_yield
        - symbol_cumprod: The cumulative product of (1 + return)

    """
    # Making start and end datetime aware
    returns_df = yh.get_symbol_data(symbol)
    returns_df = returns_df.loc[(returns_df.index.date >= start.date()) & (returns_df.index.date <= end.date())]
    returns_df.loc[:, "pct_change"] = returns_df["Close"].pct_change()
    returns_df.loc[:, "div_yield"] = returns_df["Dividends"] / returns_df["Close"]
    returns_df.loc[:, "return"] = returns_df["pct_change"] + returns_df["div_yield"]
    returns_df.loc[:, "symbol_cumprod"] = (1 + returns_df["return"]).cumprod()
    returns_df.loc[returns_df.index[0], "symbol_cumprod"] = 1

    return returns_df


def calculate_returns(symbol, start=datetime(1900, 1, 1), end=datetime.now()):
    start = to_datetime_aware(start)
    end = to_datetime_aware(end)
    benchmark_df = get_symbol_returns(symbol, start, end)

    risk_free_rate = get_risk_free_rate()

    performance(benchmark_df, risk_free_rate, symbol)


def plot_indicators(
    plot_file_html="indicators.html",
    chart_markers_df=None,
    chart_lines_df=None,
    strategy_name=None,
    show_indicators=True,
):
    # If show plot is False, then we don't want to open the plot in the browser
    if not show_indicators:
        print("show_indicators is False, not creating the plot file.")
        return

    print("\nCreating indicators plot...")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    has_chart_data = False

    ###############################
    # Chart Markers
    ###############################

    def generate_marker_plotly_text(row):
        if row["detail_text"] is None:
            return "Value: " + str(row["value"])
        else:
            return "Value: " + str(row["value"]) + "<br>" + row["detail_text"]

    # Plot the chart markers
    if chart_markers_df is not None and not chart_markers_df.empty:
        chart_markers_df = chart_markers_df.copy()
        chart_markers_df["detail_text"] = chart_markers_df.apply(generate_marker_plotly_text, axis=1)

        # Loop over the marker names and create a new trace for each one
        for marker_name in chart_markers_df["name"].unique():
            # Get the marker data for this marker name
            marker_df = chart_markers_df.loc[chart_markers_df["name"] == marker_name]

            # Get the marker symbol
            marker_symbol = marker_df["symbol"].iloc[0]

            # Get the marker size
            marker_size = marker_df["size"].iloc[0]
            marker_size = marker_size if marker_size else 25

            # Create a new trace for this marker name
            fig.add_trace(
                go.Scatter(
                    x=marker_df["datetime"],
                    y=marker_df["value"],
                    mode="markers",
                    name=marker_name,
                    marker_color=marker_df["color"],
                    marker_size=marker_size,
                    marker_symbol=marker_symbol,
                    hovertemplate=f"{marker_name}<br>%{{text}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
                    text=marker_df["detail_text"],
                )
            )

        has_chart_data = True

    ###############################
    # Chart Lines
    ###############################

    def generate_line_plotly_text(row):
        if row["detail_text"] is None:
            return "Value: " + str(row["value"])
        else:
            return "Value: " + str(row["value"]) + "<br>" + row["detail_text"]

    # Plot the chart lines
    if chart_lines_df is not None and not chart_lines_df.empty:
        chart_lines_df = chart_lines_df.copy()
        chart_lines_df["detail_text"] = chart_lines_df.apply(generate_line_plotly_text, axis=1)

        # Loop over the line names and create a new trace for each one
        for line_name in chart_lines_df["name"].unique():
            # Get the line data for this line name
            line_df = chart_lines_df.loc[chart_lines_df["name"] == line_name]

            # Get the color for this line name
            color = line_df["color"].iloc[0]

            # Create a new trace for this line name
            fig.add_trace(
                go.Scatter(
                    x=line_df["datetime"],
                    y=line_df["value"],
                    mode="lines",
                    name=line_name,
                    line_color=color,
                    hovertemplate=f"{line_name}<br>%{{text}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
                    text=line_df["detail_text"],
                )
            )

        has_chart_data = True

    ###############################
    # Chart Titles and Layouts
    ###############################

    if has_chart_data:
        # Set title and layout
        fig.update_layout(
            title_text=f"Indicators for {strategy_name}",
            title_font_size=30,
            template="plotly_dark",
            xaxis_rangeselector_font_color="black",
            xaxis_rangeselector_activecolor="grey",
            xaxis_rangeselector_bgcolor="white",
        )

        # Set y-axes titles
        fig.update_yaxes(title_text="Axis 1", secondary_y=False)
        fig.update_yaxes(title_text="Axis 2", secondary_y=True)
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
        )

        # Create graph
        fig.write_html(plot_file_html, auto_open=show_indicators)


def plot_returns(
    strategy_df,
    strategy_name,
    benchmark_df,
    benchmark_name,
    plot_file_html="backtest_result.html",
    trades_df=None,
    show_plot=True,
    initial_budget=1,
    # chart_markers_df=None,
    # chart_lines_df=None,
):
    # If show plot is False, then we don't want to open the plot in the browser
    if not show_plot:
        print("show_plot is False, not creating the plot file.")
        return

    print("\nCreating trades plot...")

    dfs_concat = []

    _df1 = strategy_df.copy()
    _df1 = _df1.sort_index(ascending=True)
    _df1.index.name = "datetime"
    _df1[strategy_name] = (1 + _df1["return"]).cumprod()
    _df1.loc[_df1.index[0], strategy_name] = 1
    _df1[strategy_name] = _df1[strategy_name] * initial_budget
    dfs_concat.append(_df1)

    _df2 = benchmark_df.copy()
    _df2 = _df2.sort_index(ascending=True)
    _df2.index.name = "datetime"
    _df2[benchmark_name] = (1 + _df2["return"]).cumprod()

    _df2.loc[_df2.index[0], benchmark_name] = 1
    _df2[benchmark_name] = _df2[benchmark_name] * initial_budget

    dfs_concat.append(_df2[benchmark_name])
    df_final = pd.concat(dfs_concat, join="outer", axis=1)

    # Make all the benchmark_df columns lowercase
    benchmark_df.columns = benchmark_df.columns.str.lower()

    # Get the ratio of the strategy to the initial_budget
    close_ratio = initial_budget / benchmark_df["close"].iloc[0]
    open_ratio = initial_budget / benchmark_df["open"].iloc[0]
    high_ratio = initial_budget / benchmark_df["high"].iloc[0]
    low_ratio = initial_budget / benchmark_df["low"].iloc[0]

    df_final["Close"] = benchmark_df["close"] * close_ratio
    df_final["Open"] = benchmark_df["open"] * open_ratio
    df_final["High"] = benchmark_df["high"] * high_ratio
    df_final["Low"] = benchmark_df["low"] * low_ratio

    if trades_df is None or trades_df.empty:
        logging.info("There were no trades in this backtest.")
        return
    else:
        trades_df = trades_df.set_index("time")
        df_final = df_final.merge(trades_df, how="outer", left_index=True, right_index=True)

    # Fix for minute timeframe backtests plotting
    # Converted to DatetimeIndex because index becomes Index type and UTC timezone in pd.concat
    # The x-axis is not displayed correctly in plotly when not converted to DatetimeIndex type
    df_final.index = pd.to_datetime(df_final.index,utc=True).tz_convert(LUMIBOT_DEFAULT_TIMEZONE)

    # fig = go.Figure()
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Strategy line
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[strategy_name],
            mode="lines",
            name=strategy_name,
            connectgaps=True,
            hovertemplate=f"{strategy_name}<br>Portfolio Value: %{{y:$,.2f}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
        )
    )

    # Benchmark line
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[benchmark_name],
            mode="lines",
            name=benchmark_name,
            connectgaps=True,
            hovertemplate=f"{benchmark_name}<br>Portfolio Value: %{{y:$,.2f}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
        )
    )

    # Cash line
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final["cash"],
            mode="lines",
            name="cash",
            connectgaps=True,
            hovertemplate="Cash<br>Value: %{y:$,.2f}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
        ),
        secondary_y=True,
    )

    # Use a % of the range of df_final[strategy_name] to shift the buy and sell ticks
    _max = df_final[strategy_name].max()
    _min = df_final[strategy_name].min()
    vshift = (_max - _min) * 0.10

    # Buy ticks
    buys = df_final.copy()
    buys[strategy_name] = buys[strategy_name].bfill()
    buys = buys.loc[df_final["side"] == "buy"]

    def generate_buysell_plotly_text(row):
        if row["status"] != "canceled" and row["status"] != "new":
            if row["asset.asset_type"] == "option":
                return (
                    row["status"]
                    + "<br>"
                    + str(row["filled_quantity"].quantize(Decimal("0.01")).__format__(",f"))
                    + " "
                    + row["symbol"]
                    + " "
                    + row["asset.right"]
                    + " Option"
                    + "<br>"
                    + "Strike: "
                    + str(row["asset.strike"])
                    + "<br>"
                    + "Expiration: "
                    + str(row["asset.expiration"])
                    + "<br>"
                    + "Price: "
                    + str(Decimal(row["price"]).quantize(Decimal("0.0001")).__format__(",f"))
                    + "<br>"
                    + "Order Type: "
                    + row["type"]
                    + "<br>"
                    + "Amount Transacted: "
                    + str(
                        # Round to 2 decimal places and add commas for thousands
                        (
                            (Decimal(row["price"]) if row["price"] else 0)
                            * (Decimal(row["filled_quantity"]) if row["filled_quantity"] else 0)
                            * (Decimal(row["asset.multiplier"]) if row["asset.multiplier"] else 0)
                        )
                        .quantize(Decimal("0.01"))
                        .__format__(",f")
                    )
                    + "<br>"
                    + "Trade Cost: "
                    + str(Decimal(row["trade_cost"]).quantize(Decimal("0.01")).__format__(",f"))
                    + "<br>"
                )
            else:
                return (
                    row["status"]
                    + "<br>"
                    + str(row["filled_quantity"].quantize(Decimal("0.01")).__format__(",f"))
                    + " "
                    + row["symbol"]
                    + "<br>"
                    + "Price: "
                    + str(Decimal(row["price"]).quantize(Decimal("0.0001")).__format__(",f"))
                    + "<br>"
                    + "Order Type: "
                    + row["type"]
                    + "<br>"
                    + "Amount Transacted: "
                    + str(
                        # Round to 2 decimal places and add commas for thousands
                        (
                            (Decimal(row["price"]) if row["price"] else 0)
                            * (Decimal(row["filled_quantity"]) if row["filled_quantity"] else 0)
                            * (Decimal(row["asset.multiplier"]) if row["asset.multiplier"] else 0)
                        )
                        .quantize(Decimal("0.01"))
                        .__format__(",f")
                    )
                    + "<br>"
                    + "Trade Cost: "
                    + str(Decimal(row["trade_cost"]).quantize(Decimal("0.01")).__format__(",f"))
                    + "<br>"
                )
        else:
            return None

    buy_ticks_df = buys.apply(generate_buysell_plotly_text, axis=1)

    # Plot the buy ticks
    if not buy_ticks_df.empty:
        buys["plotly_text_buys"] = buy_ticks_df

        # Remove any rows that have a None value for plotly_text_buys
        buys = buys.loc[buys["plotly_text_buys"].notnull()]

        buys.index.name = "datetime"
        buys = (
            buys.groupby(["datetime", strategy_name])["plotly_text_buys"].apply(lambda x: "<br>".join(x)).reset_index()
        )
        buys = buys.set_index("datetime")
        buys["buy_shift"] = buys[strategy_name] - vshift
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
                text=buys["plotly_text_buys"],
            )
        )

    ###############################
    # Plot the sell ticks
    ###############################

    # Sell ticks
    sells = df_final.copy()
    sells[strategy_name] = sells[strategy_name].bfill()
    sells = sells.loc[df_final["side"] == "sell"]

    sells_ticks_df = sells.apply(generate_buysell_plotly_text, axis=1)

    # Plot the sell ticks
    if not sells_ticks_df.empty:
        sells["plotly_text_sells"] = sells_ticks_df

        # Remove any rows that have a None value for plotly_text_sells
        sells = sells.loc[sells["plotly_text_sells"].notnull()]

        sells.index.name = "datetime"
        sells = (
            sells.groupby(["datetime", strategy_name], group_keys=True)["plotly_text_sells"]
            .apply(lambda x: "<br>".join(x))
            .reset_index()
        )
        sells = sells.set_index("datetime")
        sells["sell_shift"] = sells[strategy_name] + vshift
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
                text=sells["plotly_text_sells"],
            )
        )

    ###############################
    # Chart Titles and Layouts
    ###############################

    # Set title and layout
    bm_text = f"Compared With {benchmark_name}" if benchmark_name else ""
    fig.update_layout(
        title_text=f"{strategy_name} {bm_text}",
        title_font_size=30,
        template="plotly_dark",
        xaxis_rangeselector_font_color="black",
        xaxis_rangeselector_activecolor="grey",
        xaxis_rangeselector_bgcolor="white",
    )

    # Set y-axes titles
    fig.update_yaxes(title_text="Strategy/Benchmark", secondary_y=False)
    fig.update_yaxes(title_text="Cash", secondary_y=True)
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list(
                [
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all"),
                ]
            )
        ),
    )

    # Create graph
    fig.write_html(plot_file_html, auto_open=show_plot)


def create_tearsheet(
    strategy_df: pd.DataFrame,
    strat_name: str,
    tearsheet_file: str,
    benchmark_df: pd.DataFrame,
    benchmark_asset,  # This is causing a circular import: Asset,
    show_tearsheet: bool,
    risk_free_rate: float,
):
    # If show tearsheet is False, then we don't want to open the tearsheet in the browser
    if not show_tearsheet:
        print("show_tearsheet is False, not creating the tearsheet file.")
        return

    print("\nCreating tearsheet...")

    # Check if df1 or df2 are empty and return if they are
    if strategy_df is None or benchmark_df is None or strategy_df.empty or benchmark_df.empty:
        logging.error("No data to create tearsheet, skipping")
        return

    _strategy_df = strategy_df.copy()
    _benchmark_df = benchmark_df.copy()

    # Convert _strategy_df and _benchmark_df indexes to a date object instead of datetime
    _strategy_df.index = pd.to_datetime(_strategy_df.index)

    # Merge the strategy and benchmark dataframes on the index column
    df = pd.merge(_strategy_df, _benchmark_df, left_index=True, right_index=True, how="outer")

    df.index = pd.to_datetime(df.index)
    df["portfolio_value"] = df["portfolio_value"].ffill()

    # If the portfolio_value is NaN, backfill it because sometimes the benchmark starts before the strategy
    df["portfolio_value"] = df["portfolio_value"].bfill()

    df["symbol_cumprod"] = df["symbol_cumprod"].ffill()
    df.loc[df.index[0], "symbol_cumprod"] = 1

    df = df.resample("D").last()
    df["strategy"] = df["portfolio_value"].bfill().pct_change(fill_method=None).fillna(0)
    df["benchmark"] = df["symbol_cumprod"].bfill().pct_change(fill_method=None).fillna(0)

    # Merge the strategy and benchmark columns into a new dataframe called df_final
    df_final = df.loc[:, ["strategy", "benchmark"]]

    # df_final = df.loc[:, ["strategy", "benchmark"]]
    df_final.index = pd.to_datetime(df_final.index)
    df_final.index = df_final.index.tz_localize(None)

    # Check if df_final is empty and return if it is
    if df_final.empty or df_final["benchmark"].isnull().all() or df_final["strategy"].isnull().all():
        logging.warning("No data to create tearsheet, skipping")
        return

    # Uncomment for debugging
    # _df1.to_csv(f"df1.csv")
    # _df2.to_csv(f"df2.csv")
    # df.to_csv(f"df.csv")
    # df_final.to_csv(f"df_final.csv")

    bm_text = f"Compared to {benchmark_asset}" if benchmark_asset else ""
    title = f"{strat_name} {bm_text}"

    # Check if all the values are equal to 0
    if df_final["benchmark"].sum() == 0:
        logging.error("Not enough data to create a tearsheet, at least 2 days of data are required. Skipping")
        return

    # Check if all the values are equal to 0
    if df_final["strategy"].sum() == 0:
        logging.error("Not enough data to create a tearsheet, at least 2 days of data are required. Skipping")
        return

    # Set the name of the benchmark column so that quantstats can use it in the report
    df_final["benchmark"].name = str(benchmark_asset)

    # Run quantstats reports surpressing any logs because it can be noisy for no reason
    with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        qs.reports.html(
            df_final["strategy"],
            df_final["benchmark"],
            title=title,
            output=tearsheet_file,
            download_filename=tearsheet_file,  # Consider if you need a different name for clarity
            rf=risk_free_rate,
        )

    if show_tearsheet:
        url = "file://" + os.path.abspath(str(tearsheet_file))
        webbrowser.open(url)


def get_risk_free_rate(dt: datetime = None):
    try:
        result = yh.get_risk_free_rate(dt=dt)
    except Exception as e:
        logging.error(f"Error getting the risk free rate: {e}")
        result = 0

    return result
