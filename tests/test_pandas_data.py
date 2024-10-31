from tests.fixtures import pandas_data_fixture


class TestPandasData:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_spy_has_dividends(self, pandas_data_fixture):
        spy = list(pandas_data_fixture.values())[0]
        expected_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
        ]
        assert spy.df.columns.tolist() == expected_columns

