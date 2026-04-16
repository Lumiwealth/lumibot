import contextlib
import json
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

from .yahoo_helper import YahooHelper as yh

from lumibot.tools.lumibot_logger import get_logger
from lumibot.tools.parquet_utils import (
    coerce_object_columns_to_json_strings,
    is_parquet_required,
    write_parquet_with_logging,
)

logger = get_logger(__name__)

_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}

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

TRADE_EXPORT_COLUMNS = [
    "time",
    "side",
    "status",
    "filled_quantity",
    "symbol",
    "asset.asset_type",
    "asset.right",
    "asset.strike",
    "asset.expiration",
    "price",
    "type",
    "asset.multiplier",
    "trade_cost",
    "trade_slippage",
    "event_kind",
    "event_id",
    "cash_event_type",
    "cash_event_amount",
    "cash_event_currency",
    "cash_event_description",
    "cash_event_direction",
    "cash_event_reason",
    "is_external_cash_flow",
    "cash_event_raw_type",
    "cash_event_raw_subtype",
    "cash_event_broker_name",
    "cash_event_broker_event_id",
]

CASH_EVENT_MARKER_STYLES = {
    "deposit": {"label": "Deposit", "color": "#2ca02c", "symbol": "circle"},
    "withdrawal": {"label": "Withdrawal", "color": "#d62728", "symbol": "circle-open"},
    "financing_credit": {"label": "Financing Credit", "color": "#1f77b4", "symbol": "diamond"},
    "financing_debit": {"label": "Financing Debit", "color": "#ff7f0e", "symbol": "diamond-open"},
    "dividend": {"label": "Dividend", "color": "#17becf", "symbol": "star"},
    "interest": {"label": "Interest", "color": "#2ca02c", "symbol": "diamond"},
    "fee": {"label": "Fee", "color": "#d62728", "symbol": "x"},
    "tax": {"label": "Tax", "color": "#8c564b", "symbol": "x-open"},
    "journal": {"label": "Journal", "color": "#7f7f7f", "symbol": "square"},
    "adjustment": {"label": "Adjustment", "color": "#9467bd", "symbol": "square-open"},
    "other_cash": {"label": "Cash Event", "color": "#bcbd22", "symbol": "hexagon"},
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


def _classify_cash_event_marker(row: pd.Series) -> str:
    raw_type = str(row.get("cash_event_raw_type") or "").strip().lower()
    event_type = str(row.get("cash_event_type") or "other_cash").strip().lower()
    direction = str(row.get("cash_event_direction") or "").strip().lower()

    if event_type == "interest":
        if raw_type == "cash_financing_credit" or direction == "in":
            return "financing_credit"
        if raw_type == "cash_financing_debit" or direction == "out":
            return "financing_debit"
    return event_type if event_type in CASH_EVENT_MARKER_STYLES else "other_cash"


def _build_cash_event_marker_tooltip(row: pd.Series) -> str | None:
    event_type = str(row.get("cash_event_type") or "").strip()
    if not event_type:
        return None

    amount_value = row.get("cash_event_amount")
    amount_text = "0.00"
    if not pd.isna(amount_value):
        try:
            amount_text = f"{float(amount_value):,.2f}"
        except (TypeError, ValueError):
            amount_text = str(amount_value)

    currency = row.get("cash_event_currency")
    currency_text = str(currency).strip() if not pd.isna(currency) and currency is not None else "USD"
    description = row.get("cash_event_description")
    description_text = (
        str(description).strip()
        if not pd.isna(description) and description is not None and str(description).strip()
        else "No description"
    )
    reason = row.get("cash_event_reason")
    reason_text = (
        str(reason).strip()
        if not pd.isna(reason) and reason is not None and str(reason).strip()
        else "n/a"
    )
    direction = row.get("cash_event_direction")
    direction_text = (
        str(direction).strip()
        if not pd.isna(direction) and direction is not None and str(direction).strip()
        else "neutral"
    )
    raw_type = row.get("cash_event_raw_type")
    raw_type_text = (
        str(raw_type).strip()
        if not pd.isna(raw_type) and raw_type is not None and str(raw_type).strip()
        else "n/a"
    )
    external_flag = bool(row.get("is_external_cash_flow")) if not pd.isna(row.get("is_external_cash_flow")) else False

    return (
        f"{event_type.replace('_', ' ').title()}<br>"
        f"Amount: {amount_text} {currency_text}<br>"
        f"Direction: {direction_text}<br>"
        f"Reason: {reason_text}<br>"
        f"Raw Type: {raw_type_text}<br>"
        f"External Cash Flow: {external_flag}<br>"
        f"Description: {description_text}<br>"
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


def cumulative_to_period_flows(series: pd.Series | None) -> pd.Series:
    """Convert a cumulative external-cash-flow series into per-period flows.

    The input series is expected to increase for deposits and decrease for withdrawals.
    Missing values are forward-filled so sparse stats rows remain stable for downstream
    resampling and reporting.
    """

    if series is None:
        return pd.Series(dtype=float)

    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.empty:
        return pd.Series(dtype=float, index=series.index)

    numeric = numeric.ffill().fillna(0.0)
    period = numeric.diff()
    if len(period) > 0:
        period.iloc[0] = numeric.iloc[0]
    return period.astype(float)


def cash_flow_adjusted_returns(
    values: pd.Series | None,
    cumulative_external_flows: pd.Series | None = None,
) -> pd.Series:
    """Compute returns net of external cash flows.

    Formula:
        (ending_value - starting_value - net_external_flow) / starting_value

    Deposits are positive external flows and withdrawals are negative external flows.
    Account economics such as financing, dividends, and fees should remain embedded in
    `values`; only truly external cash movements should be passed via
    `cumulative_external_flows`.
    """

    if values is None:
        return pd.Series(dtype=float)

    numeric_values = pd.to_numeric(values, errors="coerce")
    if numeric_values.empty:
        return pd.Series(dtype=float, index=getattr(values, "index", None))

    if cumulative_external_flows is None:
        external_period = pd.Series(0.0, index=numeric_values.index, dtype=float)
    else:
        cumulative = pd.to_numeric(cumulative_external_flows, errors="coerce")
        cumulative = cumulative.reindex(numeric_values.index)
        external_period = cumulative_to_period_flows(cumulative)
        external_period = external_period.reindex(numeric_values.index).fillna(0.0)

    previous_values = numeric_values.shift(1)
    returns = (numeric_values - previous_values - external_period) / previous_values
    returns = returns.where(previous_values.notna())
    returns.name = "return"
    return returns.astype(float)


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


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default

    normalized = str(raw_value).strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    return default


def _safe_subplot_vertical_spacing(rows: int, default_spacing: float = 0.15, epsilon: float = 1e-6) -> float:
    # Plotly requires vertical_spacing <= 1 / (rows - 1) for multi-row layouts.
    if rows <= 1:
        return 0.0

    max_allowed = (1.0 / float(rows - 1)) - epsilon
    if max_allowed <= 0:
        return 0.0

    return min(default_spacing, max_allowed)


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
    # When show_indicators is False, skip HTML rendering but still emit CSV/parquet
    # so that required-mode parquet uploads don't fail due to missing artifacts.
    if not show_indicators:
        logger.debug("show_indicators is False; skipping HTML, emitting indicators CSV/parquet.")
        csv_file = plot_file_html.replace(".html", ".csv")
        standard_columns = [
            "datetime", "name", "plot_name", "type", "value",
            "symbol", "size", "color", "detail_text",
            "open", "high", "low", "close",
        ]
        # Build combined_df from whatever data was passed in, or empty.
        export_dfs = []
        if chart_markers_df is not None and not chart_markers_df.empty:
            m = chart_markers_df.copy()
            m["type"] = "marker"
            export_dfs.append(m)
        if chart_lines_df is not None and not chart_lines_df.empty:
            l = chart_lines_df.copy()
            l["type"] = "line"
            export_dfs.append(l)
        if chart_ohlc_df is not None and not chart_ohlc_df.empty:
            o = chart_ohlc_df.copy()
            o["type"] = "ohlc"
            export_dfs.append(o)
        combined_df = (
            pd.concat(export_dfs, ignore_index=True).sort_values(by="datetime")
            if export_dfs
            else pd.DataFrame(columns=standard_columns)
        )
        combined_df.to_csv(csv_file, index=False)
        parquet_file = csv_file.replace(".csv", ".parquet")
        write_parquet_with_logging(
            df=combined_df,
            path=parquet_file,
            artifact="indicators",
            logger=logger,
            index=False,
            required=is_parquet_required(),
            compression="zstd",
            sanitizer=coerce_object_columns_to_json_strings,
        )
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

    # Convert to sorted list to ensure consistent order. Ensure at least one subplot exists
    # even when the strategy emitted no chart data (empty indicators should still produce artifacts).
    plot_names = sorted(list(plot_names)) or ["default_plot"]
    num_subplots = len(plot_names)
    subplot_titles = plot_names

    vertical_spacing = _safe_subplot_vertical_spacing(num_subplots)
    if vertical_spacing < 0.15:
        logger.info(
            f"Adjusted indicators subplot vertical spacing from 0.15 to {vertical_spacing:.6f} for {num_subplots} rows."
        )

    try:
        # Create subplots without shared x-axes
        fig = make_subplots(
            rows=num_subplots,
            cols=1,
            subplot_titles=subplot_titles,
            shared_xaxes=False,  # Do not use shared x-axes
            vertical_spacing=vertical_spacing,
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

        # Set title and layout
        # Calculate height based on number of subplots
        # 400px per subplot
        height = max(800, num_subplots * 400)

        title_text = f"Indicators for {strategy_name}" if strategy_name else "Indicators"
        if not has_chart_data:
            title_text = title_text + " (no indicator data)"

        fig.update_layout(
            title_text=title_text,
            title_font_size=30,
            template="plotly_dark",
            height=height,  # Dynamic height based on number of subplots
            margin=dict(t=150),  # Add more space between title and first subplot
        )

        if has_chart_data:
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

        disable_ui = _env_flag_enabled("LUMIBOT_DISABLE_UI", default=False) or bool(os.environ.get("PYTEST_CURRENT_TEST"))
        write_indicators_html = _env_flag_enabled("LUMIBOT_WRITE_INDICATORS_HTML", default=True)

        if write_indicators_html:
            # Create graph (auto_open disabled for CI/tests).
            fig.write_html(plot_file_html, auto_open=show_indicators and not disable_ui)
        else:
            logger.info(
                "Skipping indicators HTML generation because LUMIBOT_WRITE_INDICATORS_HTML is disabled."
            )
    except Exception:
        logger.exception(
            "Indicators subplot rendering failed; continuing with indicators CSV/parquet export."
        )

    # Get the file name for the CSV file by removing the .html extension and adding .csv
    csv_file = plot_file_html.replace(".html", ".csv")

    # Export chart markers and lines to CSV - combine them and sort by datetime
    standard_columns = [
        "datetime",
        "name",
        "plot_name",
        "type",
        "value",
        "symbol",
        "size",
        "color",
        "detail_text",
        "open",
        "high",
        "low",
        "close",
    ]
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
    else:
        # Always emit indicators.csv so downstream systems can reliably query it.
        # Some strategies produce no markers/lines/OHLC; treat this as "empty indicators", not a missing artifact.
        combined_df = pd.DataFrame(columns=standard_columns)

    combined_df.to_csv(csv_file, index=False)
    parquet_file = csv_file.replace(".csv", ".parquet")
    required = is_parquet_required()
    write_parquet_with_logging(
        df=combined_df,
        path=parquet_file,
        artifact="indicators",
        logger=logger,
        index=False,
        required=required,
        compression="zstd",
        sanitizer=coerce_object_columns_to_json_strings,
    )


def plot_returns(
    strategy_df,
    strategy_name,
    benchmark_df,
    benchmark_name,
    plot_file_html="backtest_result.html",
    trades_file=None,
    trades_df=None,
    show_plot=True,
    initial_budget=1,
    # chart_markers_df=None,
    # chart_lines_df=None,
):
    disable_ui = _env_flag_enabled("LUMIBOT_DISABLE_UI", default=False) or bool(os.environ.get("PYTEST_CURRENT_TEST"))

    logger.info("\nCreating trades CSV/parquet%s...", " and plot" if show_plot else " (show_plot=False, skipping HTML)")

    # --- Start: CSV Generation for trades_df ---
    trades_csv_file = trades_file or plot_file_html.replace(".html", ".csv")
    # Define standard columns for trades data
    if trades_df is None or trades_df.empty:
        logger.info(f"No trades provided. Empty trades CSV file will be created: {trades_csv_file}")
        # Create an empty DataFrame with standard headers for the CSV
        empty_trades_for_csv = pd.DataFrame(columns=TRADE_EXPORT_COLUMNS)
        empty_trades_for_csv.to_csv(trades_csv_file, index=False)
        trades_parquet_file = trades_csv_file.replace(".csv", ".parquet")
        write_parquet_with_logging(
            df=empty_trades_for_csv,
            path=trades_parquet_file,
            artifact="trades",
            logger=logger,
            index=False,
            required=is_parquet_required(),
            compression="zstd",
            sanitizer=coerce_object_columns_to_json_strings,
        )
    else:
        # Prepare a copy of trades_df for CSV export, ensuring standard columns
        trades_df_for_csv = trades_df.copy()
        # Add any missing standard columns (filled with NA)
        for col in TRADE_EXPORT_COLUMNS:
            if col not in trades_df_for_csv.columns:
                trades_df_for_csv[col] = pd.NA
        # Select and reorder to standard columns, dropping any non-standard ones
        trades_df_for_csv = trades_df_for_csv[TRADE_EXPORT_COLUMNS]
        trades_df_for_csv.to_csv(trades_csv_file, index=False)
        logger.info(f"Trades data saved to CSV: {trades_csv_file}")
        trades_parquet_file = trades_csv_file.replace(".csv", ".parquet")
        write_parquet_with_logging(
            df=trades_df_for_csv,
            path=trades_parquet_file,
            artifact="trades",
            logger=logger,
            index=False,
            required=is_parquet_required(),
            compression="zstd",
            sanitizer=coerce_object_columns_to_json_strings,
        )
    # --- End: CSV Generation for trades_df ---

    # If show_plot is False, skip the HTML chart rendering but CSV/parquet artifacts
    # have already been written above so that required-mode parquet uploads succeed.
    if not show_plot:
        logger.info("show_plot is False; trades CSV/parquet written, skipping HTML plot.")
        return

    dfs_concat = []

    _df1 = strategy_df.copy()
    _df1 = _df1.sort_index(ascending=True)
    _df1.index.name = "datetime"
    adjusted_value_col = "cash_adjusted_portfolio_value"
    raw_value_col = "portfolio_value"
    adjusted_line_label = "Cash-Adjusted Portfolio Value"
    raw_line_label = "Portfolio Value"
    if adjusted_value_col not in _df1.columns:
        _df1[adjusted_value_col] = (1 + _df1["return"].fillna(0.0)).cumprod() * initial_budget
        _df1.loc[_df1.index[0], adjusted_value_col] = initial_budget
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
        _columns_for_merge = [col for col in TRADE_EXPORT_COLUMNS if col != "time"]
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
            _columns_to_ensure_in_merge = [col for col in TRADE_EXPORT_COLUMNS if col != "time"]
            for col in _columns_to_ensure_in_merge:
                if col not in processed_trades_for_merge.columns:
                    processed_trades_for_merge[col] = pd.NA
            # Select only the standard columns for merging
            processed_trades_for_merge = processed_trades_for_merge[[col for col in _columns_to_ensure_in_merge if col in processed_trades_for_merge.columns]]
        else:
            logger.warning("Trades data provided but 'time' column is missing. Cannot merge trades for plotting. Plot will not show trade markers.")
            # Fallback to empty trades for merge to avoid errors and ensure consistent columns in df_final
            _columns_for_merge = [col for col in TRADE_EXPORT_COLUMNS if col != "time"]
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

    for line_col in (adjusted_value_col, raw_value_col, "cash"):
        if line_col in df_final.columns:
            df_final[line_col] = pd.to_numeric(df_final[line_col], errors="coerce").ffill().bfill()

    # Cash-adjusted portfolio line (primary)
    fig.add_trace(
        go.Scatter(
            x=df_final.index,
            y=df_final[adjusted_value_col],
            mode="lines",
            name=adjusted_line_label,
            connectgaps=True,
            hovertemplate=(
                f"{adjusted_line_label}<br>"
                "Value: %{y:$,.2f}<br>"
                "%{x|%b %d %Y %I:%M:%S %p}<br>"
                "Positions:<br>"
                "%{text}<extra></extra>"
            ),
            text=formatted_positions_list,
            line=dict(width=3),
        )
    )

    # Raw portfolio value line (secondary)
    if raw_value_col in df_final.columns:
        fig.add_trace(
            go.Scatter(
                x=df_final.index,
                y=df_final[raw_value_col],
                mode="lines",
                name=raw_line_label,
                connectgaps=True,
                hovertemplate=(
                    f"{raw_line_label}<br>"
                    "Value: %{y:$,.2f}<br>"
                    "%{x|%b %d %Y %I:%M:%S %p}<extra></extra>"
                ),
                line=dict(width=2, dash="dash"),
                opacity=0.8,
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
    _max = df_final[adjusted_value_col].max()
    _min = df_final[adjusted_value_col].min()
    value_range = _max - _min
    vshift = value_range * 0.10 if value_range else max(abs(_max), 1.0) * 0.01

    # Buy ticks
    buys = df_final.copy()
    buys[adjusted_value_col] = buys[adjusted_value_col].bfill()
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
            buys.groupby(["datetime", adjusted_value_col])["plotly_text_buys"].apply(lambda x: "<br>".join(x)).reset_index()
        )
        buys = buys.set_index("datetime")
        buys["buy_shift"] = buys[adjusted_value_col] - vshift
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
    sells[adjusted_value_col] = sells[adjusted_value_col].bfill()
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
            sells.groupby(["datetime", adjusted_value_col], group_keys=True)["plotly_text_sells"]
            .apply(lambda x: "<br>".join(x))
            .reset_index()
        )
        sells = sells.set_index("datetime")
        sells["sell_shift"] = sells[adjusted_value_col] + vshift
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
    # Plot cash-event markers
    ###############################

    cash_events = df_final.copy()
    cash_events = cash_events.loc[
        (cash_events.get("event_kind") == "cash_event") & cash_events["cash_event_type"].notnull()
    ]
    if not cash_events.empty and raw_value_col in cash_events.columns:
        cash_events["marker_group"] = cash_events.apply(_classify_cash_event_marker, axis=1)
        cash_events["plotly_text_cash_events"] = cash_events.apply(_build_cash_event_marker_tooltip, axis=1)
        cash_events = cash_events.loc[cash_events["plotly_text_cash_events"].notnull()]

        marker_offsets = {
            "deposit": -0.40,
            "withdrawal": 0.40,
            "financing_credit": -0.25,
            "financing_debit": 0.25,
            "dividend": -0.15,
            "interest": -0.10,
            "fee": 0.15,
            "tax": 0.20,
            "journal": -0.30,
            "adjustment": 0.30,
            "other_cash": 0.0,
        }

        for marker_group, marker_df in cash_events.groupby("marker_group"):
            style = CASH_EVENT_MARKER_STYLES.get(marker_group, CASH_EVENT_MARKER_STYLES["other_cash"])
            grouped = (
                marker_df.groupby([marker_df.index, raw_value_col])["plotly_text_cash_events"]
                .apply(lambda values: "<br><br>".join(values))
                .reset_index()
            )
            if "level_0" in grouped.columns and "datetime" not in grouped.columns:
                grouped = grouped.rename(columns={"level_0": "datetime"})
            grouped = grouped.set_index("datetime")
            grouped["marker_y"] = grouped[raw_value_col] + (vshift * marker_offsets.get(marker_group, 0.0))
            fig.add_trace(
                go.Scatter(
                    x=grouped.index,
                    y=grouped["marker_y"],
                    mode="markers",
                    name=style["label"],
                    marker_symbol=style["symbol"],
                    marker_color=style["color"],
                    marker_size=13,
                    hovertemplate="%{text}<br>%{x|%b %d %Y %I:%M:%S %p}<extra></extra>",
                    text=grouped["plotly_text_cash_events"],
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
    # columns, etc.). QuantStats only needs the cash-flow-adjusted return series (preferred) or
    # enough information to derive it from portfolio value + external cash flows.
    strategy_columns = []
    for column in ("return", "portfolio_value", "cash_adjustments_net_total"):
        if column in strategy_df.columns:
            strategy_columns.append(column)

    if not strategy_columns:
        return None

    try:
        _strategy_df = strategy_df.loc[:, strategy_columns].copy()
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

    strategy_returns = None
    if "return" in _strategy_df.columns:
        strategy_returns = pd.to_numeric(_strategy_df["return"], errors="coerce")
        strategy_returns.index = pd.to_datetime(strategy_returns.index)
        strategy_returns = strategy_returns.sort_index()
        strategy_returns = strategy_returns.groupby(level=0).last()
        strategy_returns = ((1.0 + strategy_returns).resample("D").prod(min_count=1) - 1.0).fillna(0.0)
        strategy_returns.name = "strategy"

    df = pd.merge(_strategy_df, _benchmark_df, left_index=True, right_index=True, how="outer")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    if "portfolio_value" in df.columns:
        df["portfolio_value"] = df["portfolio_value"].ffill()
        df["portfolio_value"] = df["portfolio_value"].bfill()

    if "symbol_cumprod" in df.columns:
        df["symbol_cumprod"] = df["symbol_cumprod"].ffill()
        first_symbol = df["symbol_cumprod"].dropna().iloc[0] if not df["symbol_cumprod"].dropna().empty else 1
    else:
        first_symbol = 1
        df["symbol_cumprod"] = 1

    df.loc[df.index[0], "symbol_cumprod"] = 1 if pd.isna(first_symbol) else first_symbol

    # Seed the resample with the true initial equity so that pct_change sees day 0 -> day 1 moves.
    # Preserve the initial cumulative external cash total as well so the first daily return does
    # not accidentally absorb initialize()-time deposits/withdrawals.
    first_strategy_idx = _strategy_df.index.min()
    if pd.notna(first_strategy_idx) and "portfolio_value" in _strategy_df.columns:
        first_strategy_idx = pd.to_datetime(first_strategy_idx)
        initial_equity = _strategy_df.loc[first_strategy_idx, "portfolio_value"]
        if isinstance(initial_equity, pd.Series):
            initial_equity = initial_equity.iloc[-1]

        initial_cash_adjustments_total = 0.0
        if "cash_adjustments_net_total" in _strategy_df.columns:
            initial_cash_adjustments_total = _strategy_df.loc[first_strategy_idx, "cash_adjustments_net_total"]
            if isinstance(initial_cash_adjustments_total, pd.Series):
                initial_cash_adjustments_total = initial_cash_adjustments_total.iloc[-1]

        anchor_idx = first_strategy_idx.normalize() - pd.Timedelta(microseconds=1)
        anchor_row = pd.DataFrame(
            {
                "portfolio_value": [initial_equity],
                "symbol_cumprod": [first_symbol if not pd.isna(first_symbol) else 1],
                "cash_adjustments_net_total": [initial_cash_adjustments_total],
            },
            index=[anchor_idx],
        )
        df = pd.concat([anchor_row, df], axis=0, sort=True)
        df = df[~df.index.duplicated(keep="last")]

    # Resample to daily cadence and forward-fill non-trading days.
    # NOTE: Use forward-fill (not backfill) so weekends/holidays carry the last known value.
    # Backfilling would leak future values into prior days and can distort volatility-matched charts.
    df = df.resample("D").last()
    if "portfolio_value" in df.columns:
        df["portfolio_value"] = df["portfolio_value"].ffill()
    df["symbol_cumprod"] = df["symbol_cumprod"].ffill()

    if strategy_returns is not None:
        df["strategy"] = strategy_returns.reindex(df.index).fillna(0.0)
    else:
        cumulative_external_flows = None
        if "cash_adjustments_net_total" in df.columns:
            df["cash_adjustments_net_total"] = pd.to_numeric(
                df["cash_adjustments_net_total"], errors="coerce"
            ).ffill().fillna(0.0)
            cumulative_external_flows = df["cash_adjustments_net_total"]

        df["strategy"] = cash_flow_adjusted_returns(
            df["portfolio_value"],
            cumulative_external_flows,
        ).fillna(0.0)

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
    tearsheet_metrics_file: str | None = None,
    custom_metrics: dict | None = None,
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

    def _write_tearsheet_metrics_json(reason: str, scalar_metrics: dict | None = None) -> None:
        """Write a minimal machine-readable tearsheet metrics artifact."""
        if not tearsheet_metrics_file:
            return
        payload = {
            "metadata": {
                "summary_only": True,
                "status": "unavailable",
                "reason": reason,
            },
            "scalar_metrics": scalar_metrics or {},
        }
        try:
            with open(str(tearsheet_metrics_file), "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "Failed to write placeholder tearsheet metrics JSON to %s: %s",
                tearsheet_metrics_file,
                exc,
            )

    df_final = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    if df_final is None:
        logger.warning("No data to create tearsheet; writing placeholder and skipping QuantStats.")
        _write_placeholder_tearsheet("Insufficient data to compute strategy/benchmark return series for this window.")
        _write_tearsheet_metrics_json("insufficient_data")
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
        _write_tearsheet_metrics_json("degenerate_returns")
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
                custom_metrics=custom_metrics,
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
                        custom_metrics=custom_metrics,
                    )
                retried = True
            except Exception as retry_exc:
                logger.warning("QuantStats retry (disable KDE) failed: %s", retry_exc)

        if not retried:
            _write_placeholder_tearsheet(f"QuantStats error: {exc}")
            _write_tearsheet_metrics_json(f"quantstats_error: {exc}")
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

    def _coerce_tearsheet_metric_value(value):
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, np.generic):
            return value.item()
        if pd.isna(value):
            return None
        return value

    def _write_tearsheet_metrics_json_fallback() -> None:
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            metrics_df = qs.reports.metrics(
                df_final["strategy"],
                df_final["benchmark"],
                rf=risk_free_rate,
                display=False,
                custom_metrics=custom_metrics,
            )

        if not isinstance(metrics_df, pd.DataFrame) or metrics_df.empty:
            raise ValueError("quantstats_lumi.reports.metrics did not return a metrics DataFrame")

        metric_col = "Metric" if "Metric" in metrics_df.columns else None
        value_columns = [col for col in metrics_df.columns if col != metric_col]
        if not value_columns:
            raise ValueError("metrics DataFrame did not contain any value columns")

        strategy_col = "Strategy" if "Strategy" in value_columns else value_columns[-1]
        benchmark_col = next((col for col in value_columns if col != strategy_col), None)

        scalar_metrics: dict[str, object] = {}
        benchmark_scalar_metrics: dict[str, object] = {}
        iterable = metrics_df.iterrows()
        for idx, row in iterable:
            metric_name = row.get(metric_col) if metric_col else idx
            if metric_name is None or str(metric_name).strip() == "":
                continue
            metric_name = str(metric_name)
            scalar_metrics[metric_name] = _coerce_tearsheet_metric_value(row.get(strategy_col))
            if benchmark_col is not None:
                benchmark_scalar_metrics[metric_name] = _coerce_tearsheet_metric_value(row.get(benchmark_col))

        payload = {
            "metadata": {
                "summary_only": True,
                "status": "ok",
                "source": "metrics_fallback",
            },
            "scalar_metrics": scalar_metrics,
        }
        if benchmark_scalar_metrics:
            payload["benchmark_scalar_metrics"] = benchmark_scalar_metrics

        with open(str(tearsheet_metrics_file), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    # Generate machine-readable tearsheet metrics JSON alongside the HTML tearsheet.
    if tearsheet_metrics_file:
        try:
            metrics_json_fn = getattr(qs.reports, "metrics_json", None)
            if callable(metrics_json_fn):
                with open(os.devnull, "w") as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                    metrics_json_fn(
                        df_final["strategy"],
                        df_final["benchmark"],
                        rf=risk_free_rate,
                        output=tearsheet_metrics_file,
                        summary_only=True,
                        custom_metrics=custom_metrics,
                    )
            else:
                _write_tearsheet_metrics_json_fallback()
            logger.info("Tearsheet metrics JSON saved to %s", tearsheet_metrics_file)
        except Exception as exc:
            logger.warning("Failed to generate tearsheet metrics JSON: %s", exc)
            _write_tearsheet_metrics_json(f"metrics_json_error: {exc}")

    disable_ui = _env_flag_enabled("LUMIBOT_DISABLE_UI", default=False) or bool(os.environ.get("PYTEST_CURRENT_TEST"))

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
