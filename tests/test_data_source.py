from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset


class DataSourceTestable(DataSource):
    def __init__(self, api_key):
        super().__init__(api_key=api_key)

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        return {}

    def get_last_price(self, asset, quote=None, exchange=None):
        return 0.0

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        pass


class TestDataSource:
    def test_get_chain_full_info(self, mocker):
        ds = DataSourceTestable(api_key='test')
        chains = {'Chains': {
            "PUT": {
                "2023-12-01": [101, 102, 103],
            },
            "CALL": {
                "2023-12-01": [101, 102, 103],
            },
        }}
        mocker.patch.object(ds, 'get_chains', return_value=chains)
        mocker.patch.object(ds, 'get_last_price', return_value=1.0)

        asset = Asset("SPY")
        df_chain = ds.get_chain_full_info(asset, '2023-12-01', underlying_price=102, risk_free_rate=0.01)
        assert len(df_chain) == 6
        assert 'last' in df_chain.columns
        assert 'bid' in df_chain.columns
        assert 'greeks.delta' in df_chain.columns

        # Test with strike filters
        df_chain = ds.get_chain_full_info(asset, '2023-12-01', chains=chains, underlying_price=102,
                                          risk_free_rate=0.01, strike_min=102, strike_max=102)
        assert len(df_chain) == 2
