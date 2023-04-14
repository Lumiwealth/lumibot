Parameters
===================================

Parameters are an important part of any lumibot program and allow you to customize your strategy to your liking. Parameters are defined in the strategy file and can be accessed by the strategy methods. Parameters can be accessed by the strategy methods by using the get_parameters() method. The get_parameters() method returns a dictionary of all the parameters defined in the strategy file. The parameters can be accessed by using the parameter name as the key. For example, here's a typical strategy file that defines several parameters:

.. code-block:: python

    class MyStrategy(Strategy):
        ############################
        # Defining the initial parameters
        ############################

        # You can define the initial parameters for your strategy by using the following code
        parameters = {
            "my_parameter": 10,
            "main_ticker": "SPY",
            "ema_threshold": 10.5
        }
        
        def on_trading_iteration(self):
            ############################
            # Getting the parameters
            ############################

            # You can get a parameter by using the following code
            my_parameter = self.get_parameters()["my_parameter"]
            # Or you can get a parameter by using the following code
            main_ticker = self.parameters["main_ticker"]

            ############################
            # Setting the parameters
            ############################

            # You can set a parameter by using the following code
            self.parameters["my_parameter"] = 20
            # Or you can set a parameter by using the following code
            self.set_parameters({"my_parameter": 20})



.. currentmodule:: strategies.strategy.Strategy

.. autosummary::
    :toctree: strategy_methods.account
    :template: strategy_methods_template.rst

    get_parameters
    set_parameters