import pytest

from lumibot.tools.black_scholes import norm


@pytest.mark.parametrize(
    ("value", "expected_cdf", "expected_pdf"),
    [
        (-3.0, 0.0013498980316300933, 0.0044318484119380075),
        (-1.0, 0.15865525393145707, 0.24197072451914337),
        (0.0, 0.5, 0.3989422804014327),
        (1.0, 0.8413447460685429, 0.24197072451914337),
        (3.0, 0.9986501019683699, 0.0044318484119380075),
    ],
)
def test_normal_distribution_matches_standard_normal_table(value, expected_cdf, expected_pdf):
    assert norm.cdf(value) == pytest.approx(expected_cdf, abs=1e-12)
    assert norm.pdf(value) == pytest.approx(expected_pdf, abs=1e-12)
