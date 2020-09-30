from datetime import datetime
import math

def cagr(_df):
    df = _df.copy()
    df = df.sort_index(ascending=True)
    df["cum_return"] = (1 + df["return"]).cumprod()
    total_ret = df['cum_return'][-1]
    start = datetime.utcfromtimestamp(df.index.values[0].astype('O')/1e9)
    end = datetime.utcfromtimestamp(df.index.values[-1].astype('O')/1e9)
    period_years = (end - start).days / 365.25
    CAGR = (total_ret)**(1/period_years) - 1
    return CAGR

def volatility(_df):
    df = _df.copy()
    start = datetime.utcfromtimestamp(df.index.values[0].astype('O')/1e9)
    end = datetime.utcfromtimestamp(df.index.values[-1].astype('O')/1e9)
    period_years = (end - start).days / 365.25
    ratio_to_annual = df['return'].count() / period_years
    vol = df["return"].std() * math.sqrt(ratio_to_annual)
    return vol

def sharpe(_df, risk_free_rate):
    ret = cagr(_df)
    vol = volatility(_df)
    sharpe = (ret - risk_free_rate) / vol
    return sharpe

def max_drawdown(_df):
    df = _df.copy()
    df = df.sort_index(ascending=True)
    df["cum_return"] = (1 + df["return"]).cumprod()
    df["cum_return_max"] = df["cum_return"].cummax()
    df["drawdown"] = df["cum_return_max"] - df["cum_return"]
    df["drawdown_pct"] = df["drawdown"] / df["cum_return_max"]
    max_dd = df.loc[df["drawdown_pct"].idxmax()]
    return max_dd['drawdown_pct']

def romad(_df):
    ret = cagr(_df)
    mdd = max_drawdown(_df)
    romad = ret / mdd
    return romad
