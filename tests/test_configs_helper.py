from lumibot.components.configs_helper import ConfigsHelper
from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order


class TestConfigsHelper:

    def test_get_classic_60_40_config(self):
        """Test getting the classic 60/40 configuration"""

        configs_helper = ConfigsHelper(configs_folder="example_strategies")
        config = configs_helper.load_config("classic_60_40_config")
        assert config is not None

        assert config["market"] == "NYSE"
        assert config["sleeptime"] == "1D"
        assert config["drift_type"] == DriftType.RELATIVE
        assert config["drift_threshold"] == "0.1"
        assert config["order_type"] == Order.OrderType.MARKET
        assert config["acceptable_slippage"] == "0.005"
        assert config["fill_sleeptime"] == 15
        assert config["target_weights"] == {
            "SPY": "0.60",
            "TLT": "0.40"
        }
        assert config["shorting"] == False