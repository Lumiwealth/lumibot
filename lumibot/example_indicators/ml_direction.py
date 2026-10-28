"""A small machine-learning indicator: probability that the next bar closes up.

It trains a logistic regression (plain numpy, no extra packages) on the
returns of the bars it has already seen, and retrains every
``retrain_every`` bars. The features are the last ``lookback`` bar returns;
the label is whether the following bar went up. Training only uses bars at or
before the current one, so the indicator never sees the future.

This is a teaching example of how to put a model inside an indicator, not a
trading signal. Expect a value near 50 most of the time on real markets.
"""

from __future__ import annotations

from collections import deque

import numpy as np

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register


def _fit_logistic(x: np.ndarray, y: np.ndarray, iterations: int = 200, rate: float = 0.5, l2: float = 1e-2):
    """Gradient-descent logistic regression. Returns (weights, bias, mean, std)."""
    mean, std = x.mean(axis=0), x.std(axis=0)
    std[std == 0] = 1.0
    z = (x - mean) / std
    w, b = np.zeros(z.shape[1]), 0.0
    for _ in range(iterations):
        p = 1.0 / (1.0 + np.exp(-(z @ w + b)))
        error = p - y
        w -= rate * (z.T @ error / len(y) + l2 * w)
        b -= rate * error.mean()
    return w, b, mean, std


@register
class MLDirection(Indicator):
    key = "ml_direction"
    version = 1
    title = "ML Next-Bar Direction"
    short_title = "MLD"
    overlay = False

    inputs = [
        InputSpec("lookback", "int", 5, min=1, max=50, label="Return lags"),
        InputSpec("train_window", "int", 200, min=20, label="Training bars"),
        InputSpec("retrain_every", "int", 20, min=1, label="Retrain every N bars"),
    ]
    outputs = [OutputSpec("prob_up", "Probability up %", "line", "#10B981")]
    levels = [Level(50, "Coin flip")]

    def init_state(self):
        n = self.inputs["train_window"] + self.inputs["lookback"] + 1
        return {"closes": deque(maxlen=n), "model": None, "since_train": 0}

    def step(self, state, bar):
        closes = state["closes"]
        closes.append(bar.close)
        lookback = self.inputs["lookback"]
        returns = np.diff(np.log(np.asarray(closes, dtype=float)))
        if len(returns) < lookback:
            return {"prob_up": None}
        state["since_train"] += 1
        samples = len(returns) - lookback
        if samples >= 2 * lookback + 10 and (state["model"] is None or state["since_train"] >= self.inputs["retrain_every"]):
            # row k: returns[k:k+lookback] -> did returns[k+lookback] go up?
            x = np.lib.stride_tricks.sliding_window_view(returns, lookback)[:samples]
            y = (returns[lookback:] > 0).astype(float)
            if 0 < y.mean() < 1:
                state["model"] = _fit_logistic(x, y)
                state["since_train"] = 0
        if state["model"] is None:
            return {"prob_up": None}
        w, b, mean, std = state["model"]
        latest = (returns[-lookback:] - mean) / std
        return {"prob_up": 100.0 / (1.0 + np.exp(-(latest @ w + b)))}
