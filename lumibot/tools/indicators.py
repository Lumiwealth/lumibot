import contextlib
import math
import os
import webbrowser
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import pytz
import quantstats_lumi as qs
from plotly.subplots import make_subplots

from ..constants import LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools import to_datetime_aware
from plotly.subplots import make_subplots

from .yahoo_helper import YahooHelper as yh

from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

TERMINAL_TRADE_STATUSES_FOR_MARKERS = {
    "fill",
    "filled",
    "partial_fill",
    "cash_settled",
    "assigned",
    "assignment",
    "exercise",
    "exercised",
    "expired",
    "expire",
}


def _format_indicator_plotly_text(value: object, detail_text: object) -> str:
    """Format plotly hover text for indicator markers/lines.

    Strategies frequently omit `detail_text` for some points. When those points are collected
    into a pandas DataFrame, missing values are represented as `NaN` (a float), not `None`.
    This helper treats None/NaN/NA/empty strings as "no detail text" and always returns a
    string, never raising.
    """

    base = "Value: " + str(value)

    if detail_text is None:
        return base

    try:
        if bool(pd.isna(detail_text)):
            return base
    except Exception:
        # `pd.isna(list)` returns an array; `bool(array)` raises. Treat those as not-missing.
        pass

    detail_str = str(detail_text)
    if detail_str.strip() == "":
        return base

    return base + "<br>" + detail_str


def _build_trade_marker_tooltip(row: pd.Series):
    """Return tooltip text for a trade marker; None when the row lacks required data."""
    status_value = row.get("status")
    if pd.isna(status_value) or str(status_value).strip() == "":
        return None

    status_text = str(status_value)
    if status_text.lower() not in TERMINAL_TRADE_STATUSES_FOR_MARKERS:
        return None

    for key in ("filled_quantity", "price"):
        value = row.get(key)
        if pd.isna(value):
            return None

    try:
        filled_quantity_dec = Decimal(str(row["filled_quantity"]))
        price_dec = Decimal(str(row["price"]))
    except (InvalidOperation, TypeError, ValueError):
        return None

    multiplier_value = row.get("asset.multiplier")
    if pd.isna(multiplier_value) or multiplier_value == "":
        return None
    try:
        multiplier_dec = Decimal(str(multiplier_value))
    except (InvalidOperation, TypeError, ValueError):
        return None

    try:
        amount_transacted_dec = price_dec * filled_quantity_dec * multiplier_dec
    except (InvalidOperation, TypeError, ValueError):
        return None

    trade_cost_value = row.get("trade_cost")
    trade_cost_dec = None
    if not (pd.isna(trade_cost_value) or trade_cost_value == ""):
        try:
            trade_cost_dec = Decimal(str(trade_cost_value))
        except (InvalidOperation, TypeError, ValueError):
            trade_cost_dec = None

    if trade_cost_dec is None:
        trade_cost_dec = amount_transacted_dec

    slippage_value = row.get("trade_slippage")
    slippage_dec = None
    if not (pd.isna(slippage_value) or slippage_value == ""):
        try:
            slippage_dec = Decimal(str(slippage_value))
        except (InvalidOperation, TypeError, ValueError):
            slippage_dec = None

    if row.get("asset.asset_type") == "option":
        try:
            return (
                status_text
                + "<br>"
                + str(filled_quantity_dec.quantize(Decimal("0.01")).__format__(",f"))
                + " "
                + str(row.get("symbol"))
                + " "
                + str(row.get("asset.right"))
                + " Option"
                + "<br>"
                + "Strike: "
                + str(row.get("asset.strike"))
                + "<br>"
                + "Expiration: "
                + str(row.get("asset.expiration"))
                + "<br>"
                + "Price: "
                + str(price_dec.quantize(Decimal("0.0001")).__format__(",f"))
                + "<br>"
                + "Order Type: "
                + str(row.get("type"))
                + "<br>"
                + "Amount Transacted: "
                + str(
                    (
                        price_dec
                        * filled_quantity_dec
                        * (multiplier_dec if multiplier_dec != Decimal("0") else Decimal("1"))
                    )
                    .quantize(Decimal("0.01"))
                    .__format__(",f")
                )
                + "<br>"
                + "Trade Cost: "
                + str(trade_cost_dec.quantize(Decimal("0.01")).__format__(",f"))
                + "<br>"
                + "Slippage: "
                + (
                    str(slippage_dec.quantize(Decimal("0.01")).__format__(",f"))
                    if slippage_dec is not None
                    else "0.00"
                )
                + "<br>"
            )
        except (InvalidOperation, TypeError, ValueError):
            return None

    if multiplier_dec == Decimal("0"):
        return None

    try:
        amount_transacted = amount_transacted_dec.quantize(Decimal("0.01")).__format__(",f")
        price_text = str(price_dec.quantize(Decimal("0.0001")).__format__(",f"))
        filled_qty_text = str(filled_quantity_dec.quantize(Decimal("0.01")).__format__(",f"))
        trade_cost_text = str(trade_cost_dec.quantize(Decimal("0.01")).__format__(",f"))
        slippage_text = (
            str(slippage_dec.quantize(Decimal("0.01")).__format__(",f"))
            if slippage_dec is not None
            else "0.00"
        )
    except (InvalidOperation, TypeError, ValueError):
        return None

    return (
        status_text
        + "<br>"
        + filled_qty_text
        + " "
        + str(row.get("symbol"))
        + "<br>"
        + "Price: "
        + price_text
        + "<br>"
        + "Order Type: "
        + str(row.get("type"))
        + "<br>"
        + "Amount Transacted: "
        + amount_transacted
        + "<br>"
        + "Trade Cost: "
        + trade_cost_text
        + "<br>"
        + "Slippage: "
        + slippage_text
        + "<br>"
    )


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
    try:
        start = pd.Timestamp(df.index[0])
        end = pd.Timestamp(df.index[-1])
        if start.tzinfo is None:
            start = start.tz_localize(pytz.UTC)
        else:
            start = start.tz_convert(pytz.UTC)
        if end.tzinfo is None:
            end = end.tz_localize(pytz.UTC)
        else:
            end = end.tz_convert(pytz.UTC)
        period_years = (end - start).days / 365.25
    except Exception:
        # Avoid tearing down backtests during end-of-run stats generation; return neutral CAGR.
        return 0
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
    try:
        start = pd.Timestamp(df.index[0])
        end = pd.Timestamp(df.index[-1])
        if start.tzinfo is None:
            start = start.tz_localize(pytz.UTC)
        else:
            start = start.tz_convert(pytz.UTC)
        if end.tzinfo is None:
            end = end.tz_localize(pytz.UTC)
        else:
            end = end.tz_convert(pytz.UTC)
        period_years = (end - start).days / 365.25
    except Exception:
        # Avoid tearing down backtests during end-of-run stats generation; return neutral volatility.
        return 0
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
    # Fetch the symbol data
    returns_df = yh.get_symbol_data(symbol)

    if returns_df is None:
        return None

    # Make sure we are working with a copy to avoid SettingWithCopyWarning
    returns_df = returns_df.copy()

    # Filter the DataFrame based on date range
    returns_df = returns_df.loc[(returns_df.index.date >= start.date()) & (returns_df.index.date <= end.date())]
    if returns_df.empty:
        return returns_df

    # Calculate percentage change and dividend yield
    returns_df.loc[:, "pct_change"] = returns_df["Close"].pct_change()
    returns_df.loc[:, "div_yield"] = returns_df["Dividends"] / returns_df["Close"]

    # Calculate total return and cumulative product
    returns_df.loc[:, "return"] = returns_df["pct_change"] + returns_df["div_yield"]
    returns_df.loc[:, "symbol_cumprod"] = (1 + returns_df["return"]).cumprod()

    # Set the initial cumulative product value to 1
    returns_df.loc[returns_df.index[0], "symbol_cumprod"] = 1

    return returns_df


SAFE_COLOR_CYCLE = [
    "#FF6B6B",  # coral
    "#F4A261",  # sand
    "#2EC4B6",  # teal
    "#7E57C2",  # purple
    "#F9C74F",  # gold
    "#34A0A4",  # aquamarine
    "#E63946",  # crimson
]
_BLACK_VALUES = {"black", "#000", "#000000", "rgb(0,0,0)", "rgba(0,0,0,1)"}


def _safe_color(raw_color, key_hint=""):
    """Return a color guaranteed to be visible against dark backgrounds."""
    if isinstance(raw_color, str):
        color_text = raw_color.strip().lower()
        if color_text and color_text not in _BLACK_VALUES:
            return raw_color
    if raw_color is not None and not isinstance(raw_color, str):
        return raw_color

    idx = abs(hash(key_hint)) % len(SAFE_COLOR_CYCLE)
    return SAFE_COLOR_CYCLE[idx]


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
    chart_ohlc_df=None,
    strategy_name=None,
    show_indicators=True,
):
    # If show plot is False, then we don't want to open the plot in the browser
    if not show_indicators:
        logger.debug("show_indicators is False, not creating the plot file.")
        return

    logger.info("\nCreating indicators plot...")

    # Assign "default_plot" as plot_name for markers and lines that don't have one
    if chart_markers_df is not None and not chart_markers_df.empty:
        chart_markers_df = chart_markers_df.copy()
        if "plot_name" not in chart_markers_df.columns:
            chart_markers_df["plot_name"] = "default_plot"
        else:
            chart_markers_df["plot_name"] = chart_markers_df["plot_name"].fillna("default_plot")

    if chart_lines_df is not None and not chart_lines_df.empty:
        chart_lines_df = chart_lines_df.copy()
        if "plot_name" not in chart_lines_df.columns:
            chart_lines_df["plot_name"] = "default_plot"
        else:
            chart_lines_df["plot_name"] = chart_lines_df["plot_name"].fillna("default_plot")

    if chart_ohlc_df is not None and not chart_ohlc_df.empty:
        chart_ohlc_df = chart_ohlc_df.copy()
        if "plot_name" not in chart_ohlc_df.columns:
            chart_ohlc_df["plot_name"] = "default_plot"
        else:
            chart_ohlc_df["plot_name"] = chart_ohlc_df["plot_name"].fillna("default_plot")

    # Get unique plot_names from markers and lines
    plot_names = set()

    if chart_markers_df is not None and not chart_markers_df.empty:
        plot_names.update(chart_markers_df["plot_name"].unique())

    if chart_lines_df is not None and not chart_lines_df.empty:
        plot_names.update(chart_lines_df["plot_name"].unique())

    if chart_ohlc_df is not None and not chart_ohlc_df.empty:
        plot_names.update(chart_ohlc_df["plot_name"].unique())

    # Convert to sorted list to ensure consistent order
    plot_names = sorted(list(plot_names))

    # Ensure num_subplots is at least 1 to avoid ValueError in make_subplots
    num_subplots = max(1, len(plot_names))
    subplot_titles = plot_names if num_subplots > 0 else ["default_plot"]

    # Create subplots without shared x-axes
    fig = make_subplots(
        rows=num_subplots,
        cols=1,
        subplot_titles=subplot_titles,
        shared_xaxes=False,  # Do not use shared x-axes
        vertical_spacing=0.15,  # Increase spacing between subplots to prevent range slider overlap,
    )

    has_chart_data = False

    ###############################
    # Chart Markers
    ###############################

    def generate_marker_plotly_text(row):
        return _format_indicator_plotly_text(row.get("value"), row.get("detail_text"))

    # Plot the chart markers
    if chart_markers_df is not None and not chart_markers_df.empty:
        chart_markers_df["detail_text"] = chart_markers_df.apply(generate_marker_plotly_text, axis=1)

        # Group by plot_name first, then by name
        for plot_name, plot_df in chart_markers_df.groupby("plot_name"):
            # Loop over the marker names for this plot_name
            for marker_name, group_df in plot_df.groupby("name"):
                group_df = group_df.copy()
                # Get the marker symbol
                marker_symbol = group_df["symbol"].iloc[0]

                # Determine marker size(s), falling back to sensible defaults when unspecified
                default_marker_size = 25
                raw_sizes = group_df.get("size")
                marker_size = default_marker_size

                if raw_sizes is not None:
                    marker_sizes = pd.to_numeric(raw_sizes, errors="coerce")

                    if isinstance(marker_sizes, pd.Series):
                        marker_sizes = marker_sizes.fillna(default_marker_size).clip(lower=1)
                        unique_sizes = marker_sizes.unique()
                        if len(unique_sizes) == 1:
                            marker_size = float(unique_sizes[0])
                        else:
                            marker_size = marker_sizes.tolist()
                    else:
                        if pd.isna(marker_sizes) or marker_sizes <= 0:
                            marker_size = default_marker_size
                        else:
                            marker_size = float(marker_sizes)

                if "color" not in group_df.columns:
                    group_df["color"] = None
                group_df.loc[:, "color"] = group_df["color"].apply(
                    lambda val: _safe_color(val, f"{plot_name}:{marker_name}")
                )

                # Determine which subplot to use
                row = plot_names.index(plot_name) + 1

                # Create a new trace for this marker name
                fig.add_trace(
                    go.Scatter(
                        x=group_df["datetime"],
                        y=group_df["value"],
                        mode="markers",
                        name=marker_name,
                        marker_color=group_df["color"],
                        marker_size=marker_size,
                        marker_symbol=marker_symbol,
                        hovertemplate=f"{marker_name}<br>%{{text}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
                        text=group_df["detail_text"],
                    ),
                    row=row,
                    col=1
                )

        has_chart_data = True

    ###############################
    # Chart Lines
    ###############################

    def generate_line_plotly_text(row):
        return _format_indicator_plotly_text(row.get("value"), row.get("detail_text"))

    # Plot the chart lines
    if chart_lines_df is not None and not chart_lines_df.empty:
        chart_lines_df["detail_text"] = chart_lines_df.apply(generate_line_plotly_text, axis=1)

        # Group by plot_name first, then by name
        for plot_name, plot_df in chart_lines_df.groupby("plot_name"):
            # Loop over the line names for this plot_name
            for line_name, group_df in plot_df.groupby("name"):
                if "color" not in group_df.columns:
                    group_df = group_df.assign(color=None)
                color = _safe_color(group_df["color"].iloc[0], f"{plot_name}:{line_name}")

                # Determine which subplot to use
                row = plot_names.index(plot_name) + 1

                # Create a new trace for this line name
                fig.add_trace(
                    go.Scatter(
                        x=group_df["datetime"],
                        y=group_df["value"],
                        mode="lines",
                        name=line_name,
                        line_color=color,
                        hovertemplate=f"{line_name}<br>%{{text}}<br>%{{x|%b %d %Y %I:%M:%S %p}}<extra></extra>",
                        text=group_df["detail_text"],
                    ),
                    row=row,
                    col=1
                )

        has_chart_data = True

    ###############################
    # Chart OHLC
    ###############################

    def _generate_ohlc_hover_text(row):
        base = f"O: {row['open']}<br>H: {row['high']}<br>L: {row['low']}<br>C: {row['close']}"
        if row.get("detail_text") is None:
            return base
        return base + "<br>" + str(row.get("detail_text"))

    if chart_ohlc_df is not None and not chart_ohlc_df.empty:
        chart_ohlc_df = chart_ohlc_df.copy()

        for col in ("open", "high", "low", "close"):
            if col not in chart_ohlc_df.columns:
                logger.warning(f"OHLC data missing required column '{col}', skipping OHLC plotting.")
                chart_ohlc_df = None
                break

        if chart_ohlc_df is not None and not chart_ohlc_df.empty:
            if "color" not in chart_ohlc_df.columns:
                chart_ohlc_df["color"] = None

            # Default per-bar colors: green for bullish, red for bearish (matches Strategy.add_ohlc defaults).
            chart_ohlc_df["color"] = chart_ohlc_df["color"].where(
                chart_ohlc_df["color"].notna(),
                np.where(chart_ohlc_df["close"] >= chart_ohlc_df["open"], "green", "red"),
            )

            chart_ohlc_df["detail_text"] = chart_ohlc_df.apply(_generate_ohlc_hover_text, axis=1)

            # Group by plot_name first, then by series name.
            for plot_name, plot_df in chart_ohlc_df.groupby("plot_name"):
                for ohlc_name, group_df in plot_df.groupby("name"):
                    row = plot_names.index(plot_name) + 1

                    # Preserve per-bar colors by splitting into separate traces per color.
                    color_groups = list(group_df.groupby("color"))
                    for idx, (bar_color, colored_df) in enumerate(color_groups):
                        trace_color = _safe_color(bar_color, f"{plot_name}:{ohlc_name}:{bar_color}")

                        fig.add_trace(
                            go.Candlestick(
                                x=colored_df["datetime"],
                                open=colored_df["open"],
                                high=colored_df["high"],
                                low=colored_df["low"],
                                close=colored_df["close"],
                                name=ohlc_name,
                                showlegend=idx == 0,
                                legendgroup=ohlc_name,
                                increasing_line_color=trace_color,
                                decreasing_line_color=trace_color,
                                increasing_fillcolor=trace_color,
                                decreasing_fillcolor=trace_color,
                                hovertext=colored_df["detail_text"],
                                hoverinfo="x+text",
                            ),
                            row=row,
                            col=1,
                        )

            has_chart_data = True

    ###############################
    # Chart Titles and Layouts
    ###############################

    if has_chart_data:
        # Set title and layout
        # Calculate height based on number of subplots
        # 400px per subplot
        height = max(800, num_subplots * 400)

        fig.update_layout(
            title_text=f"Indicators for {strategy_name}",
            title_font_size=30,
            template="plotly_dark",
            height=height,  # Dynamic height based on number of subplots
            margin=dict(t=150),  # Add more space between title and first subplot
        )

        # Range selector buttons
        rangeselector_buttons = list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all"),
        ])

        # Update axes for all subplots
        for i in range(1, num_subplots + 1):
            # Get the plot name for this subplot
            plot_title = plot_names[i - 1]

            # Set y-axes titles for each subplot
            fig.update_yaxes(
                title_text=plot_title,
                secondary_y=False,
                row=i,
                col=1
            )

            # Add range selector and range slider to each subplot
            fig.update_xaxes(
                rangeselector=dict(
                    buttons=rangeselector_buttons,
                    font=dict(color="black"),
                    activecolor="grey",
                    bgcolor="white",
                ),
                rangeslider=dict(
                    visible=True,
                    thickness=0.02  # Make the range slider height shorter to make line graph appear taller
                ),
                row=i,
                col=1
            )

        disable_ui = (
            os.environ.get("LUMIBOT_DISABLE_UI", "").strip().lower() in ("1", "true", "yes")
            or bool(os.environ.get("PYTEST_CURRENT_TEST"))
        )

        # Create graph (auto_open disabled for CI/tests).
        fig.write_html(plot_file_html, auto_open=show_indicators and not disable_ui)

        # Get the file name for the CSV file by removing the .html extension and adding .csv
        csv_file = plot_file_html.replace(".html", ".csv")

        # Export chart markers and lines to CSV - combine them and sort by datetime
        export_dfs = []
        if chart_markers_df is not None and not chart_markers_df.empty:
            markers_out = chart_markers_df.copy()
            markers_out["type"] = "marker"
            export_dfs.append(markers_out)
        if chart_lines_df is not None and not chart_lines_df.empty:
            lines_out = chart_lines_df.copy()
            lines_out["type"] = "line"
            export_dfs.append(lines_out)
        if chart_ohlc_df is not None and not chart_ohlc_df.empty:
            ohlc_out = chart_ohlc_df.copy()
            ohlc_out["type"] = "ohlc"
            export_dfs.append(ohlc_out)

        if export_dfs:
            combined_df = pd.concat(export_dfs, ignore_index=True).sort_values(by="datetime")
            combined_df.to_csv(csv_file, index=False)


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
        logger.info("show_plot is False, not creating the plot file or CSV.")
        return

    disable_ui = (
        os.environ.get("LUMIBOT_DISABLE_UI", "").strip().lower() in ("1", "true", "yes")
        or bool(os.environ.get("PYTEST_CURRENT_TEST"))
    )

    logger.info("\nCreating trades plot and CSV...")

    # --- Start: CSV Generation for trades_df ---
    trades_csv_file = plot_file_html.replace(".html", ".csv")
    # Define standard columns for trades data
    standard_trade_columns = [
        "time", "side", "status", "filled_quantity", "symbol", "asset.asset_type",
        "asset.right", "asset.strike", "asset.expiration", "price", "type",
        "asset.multiplier", "trade_cost", "trade_slippage"
    ]

    if trades_df is None or trades_df.empty:
        logger.info(f"No trades provided. Empty trades CSV file will be created: {trades_csv_file}")
        # Create an empty DataFrame with standard headers for the CSV
        empty_trades_for_csv = pd.DataFrame(columns=standard_trade_columns)
        empty_trades_for_csv.to_csv(trades_csv_file, index=False)
    else:
        # Prepare a copy of trades_df for CSV export, ensuring standard columns
        trades_df_for_csv = trades_df.copy()
        # Add any missing standard columns (filled with NA)
        for col in standard_trade_columns:
            if col not in trades_df_for_csv.columns:
                trades_df_for_csv[col] = pd.NA
        # Select and reorder to standard columns, dropping any non-standard ones
        trades_df_for_csv = trades_df_for_csv[standard_trade_columns]
        trades_df_for_csv.to_csv(trades_csv_file, index=False)
        logger.info(f"Trades data saved to CSV: {trades_csv_file}")
    # --- End: CSV Generation for trades_df ---

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

    # Optional: scale OHLC series into the same units as the strategy budget.
    # Some benchmark sources (e.g. IBKR fallback-to-equity-curve) intentionally provide only
    # returns/cumprod and do not include OHLC. These series are not required for the plot itself.
    if {"close", "open", "high", "low"}.issubset(set(benchmark_df.columns)):
        close_ratio = initial_budget / benchmark_df["close"].iloc[0]
        open_ratio = initial_budget / benchmark_df["open"].iloc[0]
        high_ratio = initial_budget / benchmark_df["high"].iloc[0]
        low_ratio = initial_budget / benchmark_df["low"].iloc[0]

        df_final["Close"] = benchmark_df["close"] * close_ratio
        df_final["Open"] = benchmark_df["open"] * open_ratio
        df_final["High"] = benchmark_df["high"] * high_ratio
        df_final["Low"] = benchmark_df["low"] * low_ratio

    # Prepare trades data for merging into df_final for the plot
    # `processed_trades_for_merge` will be indexed by 'time' and contain standard trade columns (excluding 'time')
    if trades_df is None or trades_df.empty:
        logger.info("There were no trades in this backtest. Plot will not show trade markers.")
        # Create a DataFrame with standard trade columns (all NaN) and df_final's index (if any)
        # This ensures df_final gets all standard trade columns for consistent plotting.
        _columns_for_merge = [col for col in standard_trade_columns if col != "time"]
        if not df_final.index.empty:
            processed_trades_for_merge = pd.DataFrame(index=df_final.index, columns=_columns_for_merge)
        else: # df_final is empty, create an empty df with columns and time index
            processed_trades_for_merge = pd.DataFrame(columns=_columns_for_merge)
            processed_trades_for_merge.index = pd.to_datetime(processed_trades_for_merge.index) # ensure datetimeindex
        processed_trades_for_merge.index.name = "time"
    else:
        # We have trades, prepare a copy
        processed_trades_for_merge = trades_df.copy()
        if 'time' in processed_trades_for_merge.columns:
            processed_trades_for_merge['time'] = pd.to_datetime(processed_trades_for_merge['time'])
            processed_trades_for_merge = processed_trades_for_merge.set_index('time')
            
            # Ensure all standard columns (excluding 'time') are present, filling missing ones with NA
            _columns_to_ensure_in_merge = [col for col in standard_trade_columns if col != "time"]
            for col in _columns_to_ensure_in_merge:
                if col not in processed_trades_for_merge.columns:
                    processed_trades_for_merge[col] = pd.NA
            # Select only the standard columns for merging
            processed_trades_for_merge = processed_trades_for_merge[[col for col in _columns_to_ensure_in_merge if col in processed_trades_for_merge.columns]]
        else:
            logger.warning("Trades data provided but 'time' column is missing. Cannot merge trades for plotting. Plot will not show trade markers.")
            # Fallback to empty trades for merge to avoid errors and ensure consistent columns in df_final
            _columns_for_merge = [col for col in standard_trade_columns if col != "time"]
            if not df_final.index.empty:
                processed_trades_for_merge = pd.DataFrame(index=df_final.index, columns=_columns_for_merge)
            else:
                processed_trades_for_merge = pd.DataFrame(columns=_columns_for_merge)
                processed_trades_for_merge.index = pd.to_datetime(processed_trades_for_merge.index)
            processed_trades_for_merge.index.name = "time"

    df_final = df_final.merge(processed_trades_for_merge, how="outer", left_index=True, right_index=True)

    # Fix for minute timeframe backtests plotting
    # Converted to DatetimeIndex because index becomes Index type and UTC timezone in pd.concat
    # The x-axis is not displayed correctly in plotly when not converted to DatetimeIndex type
    df_final.index = pd.to_datetime(df_final.index, utc=True).tz_convert(LUMIBOT_DEFAULT_TIMEZONE)

    # fig = go.Figure()
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Updated format_positions function to handle lists and dicts
    def format_positions(positions):
        if isinstance(positions, list):
            formatted_positions = [
                f"{pos.get('asset', 'Unknown asset')}: {pos.get('quantity', 0):,.2f}" for pos in positions
            ]
            return "<br>".join(formatted_positions)
        elif isinstance(positions, dict):
            return f"{positions.get('asset', 'Unknown asset')}: {positions.get('quantity', 0):,.2f}"
        return "No positions"

    # Manually create a list of formatted positions
    formatted_positions_list = [format_positions(pos) for pos in df_final["positions"]]

    # Modify the strategy line to include positions
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[strategy_name],
            mode="lines",
            name=strategy_name,
            connectgaps=True,
            hovertemplate=(
                f"{strategy_name}<br>"
                "Portfolio Value: %{y:$,.2f}<br>"
                "%{x|%b %d %Y %I:%M:%S %p}<br>"
                "Positions:<br>"
                "%{text}<extra></extra>"
            ),
            text=formatted_positions_list,  # Apply the formatting function to positions
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
    # Include all buy-type sides: buy, buy_to_open, buy_to_cover, buy_to_close
    buys = buys.loc[df_final["side"].isin(["buy", "buy_to_open", "buy_to_cover", "buy_to_close"])]

    def generate_buysell_plotly_text(row):
        return _build_trade_marker_tooltip(row)

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
    # Include all sell-type sides: sell, sell_to_close, sell_short, sell_to_open
    sells = sells.loc[df_final["side"].isin(["sell", "sell_to_close", "sell_short", "sell_to_open"])]

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

    # Create graph (auto_open disabled for CI/tests).
    fig.write_html(plot_file_html, auto_open=show_plot and not disable_ui)


def _prepare_tearsheet_returns(strategy_df: pd.DataFrame, benchmark_df: pd.DataFrame):
    if strategy_df is None or benchmark_df is None:
        return None

    if strategy_df.empty or benchmark_df.empty:
        return None

    # PERF/MEMORY: Backtests can carry very wide `strategy_df` frames (positions, orders, debug
    # columns, etc.). QuantStats only needs the portfolio value series and the benchmark cumprod.
    # Copying the full frame can spike RSS and has caused production backtests to OOM (exit code -9).
    try:
        _strategy_df = strategy_df.loc[:, ["portfolio_value"]].copy()
    except Exception:
        return None

    if "symbol_cumprod" in benchmark_df.columns:
        _benchmark_df = benchmark_df.loc[:, ["symbol_cumprod"]].copy()
    else:
        # Maintain backward-compat for benchmark frames that don't include `symbol_cumprod`.
        _benchmark_df = pd.DataFrame(index=benchmark_df.index)
        _benchmark_df["symbol_cumprod"] = 1

    _strategy_df.index = pd.to_datetime(_strategy_df.index)
    _benchmark_df.index = pd.to_datetime(_benchmark_df.index)

    df = pd.merge(_strategy_df, _benchmark_df, left_index=True, right_index=True, how="outer")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    df["portfolio_value"] = df["portfolio_value"].ffill()
    df["portfolio_value"] = df["portfolio_value"].bfill()

    if "symbol_cumprod" in df.columns:
        df["symbol_cumprod"] = df["symbol_cumprod"].ffill()
        first_symbol = df["symbol_cumprod"].dropna().iloc[0] if not df["symbol_cumprod"].dropna().empty else 1
    else:
        first_symbol = 1
        df["symbol_cumprod"] = 1

    df.loc[df.index[0], "symbol_cumprod"] = 1 if pd.isna(first_symbol) else first_symbol

    # Seed the resample with the true initial equity so that pct_change sees day 0 -> day 1 moves
    first_strategy_idx = _strategy_df.index.min()
    if pd.notna(first_strategy_idx):
        first_strategy_idx = pd.to_datetime(first_strategy_idx)
        initial_equity = _strategy_df.loc[first_strategy_idx, "portfolio_value"]
        # Some backtests record multiple portfolio snapshots at the same timestamp. In that case
        # `.loc[...]` returns a Series; pick the last value to preserve the later
        # `df.index.duplicated(keep="last")` de-dup semantics.
        if isinstance(initial_equity, pd.Series):
            initial_equity = initial_equity.iloc[-1]
        anchor_idx = first_strategy_idx.normalize() - pd.Timedelta(microseconds=1)
        anchor_row = pd.DataFrame(
            {
                "portfolio_value": [initial_equity],
                "symbol_cumprod": [first_symbol if not pd.isna(first_symbol) else 1],
            },
            index=[anchor_idx],
        )
        df = pd.concat([anchor_row, df], axis=0, sort=True)
        df = df[~df.index.duplicated(keep="last")]

    # Resample to daily cadence and forward-fill non-trading days.
    # NOTE: Use forward-fill (not backfill) so weekends/holidays carry the last known value.
    # Backfilling would leak future values into prior days and can distort volatility-matched charts.
    df = df.resample("D").last()
    df["portfolio_value"] = df["portfolio_value"].ffill()
    df["symbol_cumprod"] = df["symbol_cumprod"].ffill()
    df["strategy"] = df["portfolio_value"].pct_change(fill_method=None).fillna(0)
    df["benchmark"] = df["symbol_cumprod"].pct_change(fill_method=None).fillna(0)

    df_final = df.loc[:, ["strategy", "benchmark"]]
    df_final.index = pd.to_datetime(df_final.index)
    df_final.index = df_final.index.tz_localize(None)

    if df_final.empty or df_final["benchmark"].isnull().all() or df_final["strategy"].isnull().all():
        return None

    return df_final


def create_tearsheet(
    strategy_df: pd.DataFrame,
    strat_name: str,
    tearsheet_file: str,
    benchmark_df: pd.DataFrame,
    benchmark_asset,  # This is causing a circular import: Asset,
    show_tearsheet: bool,
    save_tearsheet: bool,
    risk_free_rate: float,
    strategy_parameters: dict = None,
    lumibot_version: str | None = None,
    backtesting_data_source: str | None = None,
    backtesting_data_sources: str | None = None,
    backtest_time_seconds: float | None = None,
):
    # If show tearsheet is False, then we don't want to open the tearsheet in the browser
    # IMS create the tearsheet even if we are not showinbg it
    if not save_tearsheet:
        logger.info("save_tearsheet is False, not creating the tearsheet file.")
        return

    logger.info("\nCreating tearsheet...")

    def _write_placeholder_tearsheet(reason: str) -> None:
        """Write a small HTML file explaining why QuantStats was skipped/failed."""
        try:
            placeholder = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>{strat_name} tearsheet unavailable</title>
  </head>
  <body>
    <h1>{strat_name}</h1>
    <p><strong>Tearsheet not generated.</strong></p>
    <p>{reason}</p>
  </body>
</html>
"""
            with open(str(tearsheet_file), "w", encoding="utf-8") as f:
                f.write(placeholder)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to write placeholder tearsheet to %s: %s", tearsheet_file, exc)

    df_final = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    if df_final is None:
        logger.warning("No data to create tearsheet; writing placeholder and skipping QuantStats.")
        _write_placeholder_tearsheet("Insufficient data to compute strategy/benchmark return series for this window.")
        return

    # Uncomment for debugging
    # _df1.to_csv(f"df1.csv")
    # _df2.to_csv(f"df2.csv")
    # df.to_csv(f"df.csv")
    # df_final.to_csv(f"df_final.csv")

    bm_text = f"Compared to {benchmark_asset}" if benchmark_asset else ""
    title = f"{strat_name} {bm_text}"

    # QuantStats (via seaborn/scipy) can raise (e.g., LinAlgError) when the return series is
    # degenerate, such as no trades and a flat portfolio value. In these cases we must not
    # crash the backtest; write a placeholder tearsheet instead.
    strategy_returns = df_final["strategy"].dropna()
    benchmark_returns = df_final["benchmark"].dropna()
    if strategy_returns.empty or benchmark_returns.empty or strategy_returns.nunique() <= 1 or benchmark_returns.nunique() <= 1:
        logger.warning(
            "Not enough return variation to generate QuantStats tearsheet (strategy unique=%d, benchmark unique=%d); writing placeholder.",
            int(strategy_returns.nunique()) if not strategy_returns.empty else 0,
            int(benchmark_returns.nunique()) if not benchmark_returns.empty else 0,
        )
        _write_placeholder_tearsheet("Return series is flat/degenerate (often caused by zero trades).")
        return

    '''
    # Check if all the values are equal to 0
    if df_final["benchmark"].sum() == 0:
        logger.error("Not enough data to create a tearsheet, at least 2 days of data are required. Skipping")
        return

    # Check if all the values are equal to 0
    if df_final["strategy"].sum() == 0:
        logger.error("Not enough data to create a tearsheet, at least 2 days of data are required. Skipping")
        return
    '''
    # Set the name of the benchmark column so that quantstats can use it in the report
    df_final["benchmark"].name = str(benchmark_asset)

    # Run quantstats reports surpressing any logs because it can be noisy for no reason
    try:
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            result = qs.reports.html(
                df_final["strategy"],
                df_final["benchmark"],
                title=title,
                output=tearsheet_file,
                download_filename=tearsheet_file,  # Consider if you need a different name for clarity
                rf=risk_free_rate,
                parameters=strategy_parameters,
                lumibot_version=lumibot_version,
                backtesting_data_source=backtesting_data_source,
                backtesting_data_sources=backtesting_data_sources,
                backtest_time_seconds=backtest_time_seconds,
            )
    except Exception as exc:
        # QuantStats can fail on short windows when seaborn tries to fit a KDE on
        # near-singular data. Retry once with the histogram KDE disabled so we still
        # produce a useful tearsheet for short/deterministic windows.
        message = str(exc)
        logger.warning("QuantStats tearsheet generation failed: %s", message)

        retried = False
        if any(token in message for token in ("gaussian_kde", "singular", "covariance matrix")):
            try:
                import quantstats_lumi._plotting.core as _qs_core
                import quantstats_lumi.plots as _qs_plots
                import quantstats_lumi.utils as _qs_utils

                def _histogram_no_kde(
                    returns,
                    benchmark=None,
                    resample="ME",
                    fontname="Arial",
                    grayscale=False,
                    figsize=(10, 5),
                    ylabel=True,
                    subtitle=True,
                    compounded=True,
                    savefig=None,
                    show=True,
                    prepare_returns=True,
                ):
                    if prepare_returns:
                        returns = _qs_utils._prepare_returns(returns)
                        if benchmark is not None:
                            benchmark = _qs_utils._prepare_returns(benchmark)

                    if resample == "W":
                        title_prefix = "Weekly "
                    elif resample == "ME":
                        title_prefix = "Monthly "
                    elif resample == "Q":
                        title_prefix = "Quarterly "
                    elif resample == "YE":
                        title_prefix = "Annual "
                    else:
                        title_prefix = ""

                    return _qs_core.plot_histogram(
                        returns,
                        benchmark,
                        resample=resample,
                        grayscale=grayscale,
                        fontname=fontname,
                        title="Distribution of %sReturns" % title_prefix,
                        kde=False,
                        figsize=figsize,
                        ylabel=ylabel,
                        subtitle=subtitle,
                        compounded=compounded,
                        savefig=savefig,
                        show=show,
                    )

                _qs_plots.histogram = _histogram_no_kde

                with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                    result = qs.reports.html(
                        df_final["strategy"],
                        df_final["benchmark"],
                        title=title,
                        output=tearsheet_file,
                        download_filename=tearsheet_file,
                        rf=risk_free_rate,
                        parameters=strategy_parameters,
                        lumibot_version=lumibot_version,
                        backtesting_data_source=backtesting_data_source,
                        backtesting_data_sources=backtesting_data_sources,
                        backtest_time_seconds=backtest_time_seconds,
                    )
                retried = True
            except Exception as retry_exc:
                logger.warning("QuantStats retry (disable KDE) failed: %s", retry_exc)

        if not retried:
            _write_placeholder_tearsheet(f"QuantStats error: {exc}")
            return

    # QuantStats occasionally emits malformed or low-precision percent cells
    # (e.g., "-" or "-11%" instead of "-11.89%"). Our CI acceptance harness pins to 0.01%
    # resolution, so normalize the headline metrics using stable computations over the exact
    # return series passed into QuantStats (df_final).
    try:
        import re

        if isinstance(result, pd.DataFrame) and "Strategy" in result.columns:
            # Acceptance baselines are pinned to 0.01% resolution.
            percent_re = re.compile(r"^-?\\d[\\d,]*\\.\\d{2}%$")

            def _is_valid_percent(value: object) -> bool:
                if value is None:
                    return False
                s = str(value).strip()
                return bool(percent_re.match(s))

            def _fmt_percent(value: float) -> str:
                return f"{float(value) * 100.0:.2f}%"

            try:
                import quantstats_lumi as _qs

                strat_returns = _qs.utils._prepare_returns(df_final["strategy"].astype(float))
                bench_returns = _qs.utils._prepare_returns(df_final["benchmark"].astype(float))

                headline_values = {
                    "Total Return": (
                        float(_qs.stats.comp(strat_returns)),
                        float(_qs.stats.comp(bench_returns)),
                    ),
                    "CAGR% (Annual Return)": (
                        float(_qs.stats.cagr(strat_returns)),
                        float(_qs.stats.cagr(bench_returns)),
                    ),
                    "Max Drawdown": (
                        float(_qs.stats.max_drawdown(strat_returns)),  # negative fraction
                        float(_qs.stats.max_drawdown(bench_returns)),  # negative fraction
                    ),
                }
            except Exception:
                headline_values = {}

            # Best-effort detect benchmark column (QuantStats names it using `df_final["benchmark"].name`).
            # QuantStats emits metrics with `Metric` as the index name, not a column.
            benchmark_cols = [c for c in result.columns if c != "Strategy"]
            benchmark_col = benchmark_cols[0] if benchmark_cols else None

            for metric_name, pair in headline_values.items():
                idx = None
                if "Metric" in result.columns:
                    row = result.index[result["Metric"] == metric_name]
                    if len(row) == 1:
                        idx = row[0]
                else:
                    if metric_name in result.index:
                        idx = metric_name
                if idx is None:
                    continue

                if not _is_valid_percent(result.at[idx, "Strategy"]):
                    result.at[idx, "Strategy"] = _fmt_percent(pair[0])

                if benchmark_col is not None and not _is_valid_percent(result.at[idx, benchmark_col]):
                    result.at[idx, benchmark_col] = _fmt_percent(pair[1])
    except Exception:  # pragma: no cover
        pass

    disable_ui = (
        os.environ.get("LUMIBOT_DISABLE_UI", "").strip().lower() in ("1", "true", "yes")
        or bool(os.environ.get("PYTEST_CURRENT_TEST"))
    )

    if show_tearsheet and not disable_ui:
        url = "file://" + os.path.abspath(str(tearsheet_file))
        webbrowser.open(url)

    return result


def get_risk_free_rate(dt: datetime = None):
    try:
        result = yh.get_risk_free_rate(dt=dt)
    except Exception as e:
        logger.error(f"Error getting the risk free rate: {e}")
        result = 0

    return result
