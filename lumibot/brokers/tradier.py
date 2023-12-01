from lumibot.brokers import Broker
from lumibot.data_sources import TRADIER_LIVE_API_URL, TRADIER_PAPER_API_URL, TradierAPIError, TradierData
from lumibot.entities import Asset, Order


class Tradier(Broker):
    """
    Broker that connects to Tradier API to place orders and retrieve data
    """
    def __init__(self, account_id=None, api_token=None, paper=True, config=None, max_workers=20, connect_stream=True,
                 data_source=None):

        if data_source is None:
            data_source = TradierData(account_id=account_id, api_key=api_token, paper=paper, max_workers=max_workers)

        super().__init__(name='Tradier', data_source=data_source, config=config, max_workers=max_workers,
                         connect_stream=connect_stream)

        self._tradier_api_key = api_token
        self._tradier_account_id = account_id
        self._tradier_paper = paper
        self._tradier_base_url = TRADIER_PAPER_API_URL if self._tradier_paper else TRADIER_LIVE_API_URL

        try:
            self.validate_credentials()
        except TradierAPIError as e:
            raise TradierAPIError("Invalid Tradier Credentials") from e

    def validate_credentials(self):
        pass

    def cancel_order(self, order: Order):
        pass

    def _submit_order(self, order: Order):
        pass

    def _get_balances_at_broker(self, quote_asset: Asset) -> float:
        pass

    def get_historical_account_value(self):
        pass

    def _get_stream_object(self):
        pass

    def _register_stream_events(self):
        pass

    def _run_stream(self):
        pass

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        pass

    def _pull_broker_position(self, asset: Asset):
        pass

    def _pull_broker_positions(self, strategy=None):
        pass

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        pass

    def _pull_broker_order(self, identifier):
        pass

    def _pull_broker_open_orders(self):
        pass
