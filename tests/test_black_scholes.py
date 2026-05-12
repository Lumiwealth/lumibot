import pytest

from lumibot.tools import black_scholes


def test_black_scholes_known_price_and_greeks() -> None:
    result = black_scholes.BS([100, 105, 5, 30], volatility=20)

    assert result.callPrice == pytest.approx(0.7307905793853493)
    assert result.putPrice == pytest.approx(5.300169174626802)
    assert result.callDelta == pytest.approx(0.2264536760489173)
    assert result.putDelta == pytest.approx(-0.7735463239510827)
    assert result.vega == pytest.approx(0.08629606057382821)
    assert result.gamma == pytest.approx(0.05249677018241216)


def test_currency_and_merton_models_keep_known_outputs() -> None:
    gk = black_scholes.GK([1.4565, 1.45, 1, 2, 30], volatility=20)
    merton = black_scholes.Me([52, 50, 1, 1, 30], volatility=20)

    assert gk.callPrice == pytest.approx(0.03591379198404554)
    assert gk.putPrice == pytest.approx(0.030614780580200285)
    assert gk.callDelta == pytest.approx(0.5359047127632695)
    assert gk.putDelta == pytest.approx(-0.46245280197803584)

    assert merton.callPrice == pytest.approx(2.3971140478112147)
    assert merton.putPrice == pytest.approx(0.4381618999049586)
    assert merton.callDelta == pytest.approx(0.7566711071710946)
    assert merton.putDelta == pytest.approx(-0.24174953016719933)


def test_implied_volatility_known_outputs() -> None:
    call_iv = black_scholes.BS([100, 105, 5, 30], callPrice=2.0)
    put_iv = black_scholes.BS([100, 105, 5, 30], putPrice=6.5)
    gk_iv = black_scholes.GK([1.4565, 1.45, 1, 2, 30], callPrice=0.0359)

    assert call_iv.impliedVolatility == pytest.approx(33.203125)
    assert put_iv.impliedVolatility == pytest.approx(32.2265625)
    assert gk_iv.impliedVolatility == pytest.approx(20.01953125)


def test_zero_volatility_returns_intrinsic_values() -> None:
    result = black_scholes.BS([100, 105, 5, 30], volatility=0)

    assert result.callPrice == 0.0
    assert result.putPrice == 5.0
    assert result.callDelta == 0.0
    assert result.putDelta == -1.0
    assert result.callTheta == 0.0
    assert result.putTheta == 0.0
    assert result.vega == 0.0
    assert result.gamma == 0.0


def test_implied_volatility_rejects_unknown_model_name() -> None:
    with pytest.raises(ValueError, match="Unsupported option model"):
        black_scholes.impliedVolatility("not_a_model", [100, 105, 5, 30], callPrice=2.0)
