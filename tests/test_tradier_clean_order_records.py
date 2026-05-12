import pandas as pd

from lumibot.brokers.tradier import Tradier


def test_clean_order_records_rounds_floats_and_nulls():
    df = pd.DataFrame(
        [
            {"id": "1", "avg_fill_price": 1.23456, "exec_quantity": 2.0, "x": pd.NA},
            {"id": "2", "avg_fill_price": float("nan"), "exec_quantity": 3.3333, "x": pd.NaT},
        ]
    )
    out = Tradier._clean_order_records(df)
    assert out[0]["avg_fill_price"] == 1.23
    assert out[0]["exec_quantity"] == 2.0
    assert out[0]["x"] is None
    assert out[1]["avg_fill_price"] is None
    assert out[1]["exec_quantity"] == 3.33
    assert out[1]["x"] is None
