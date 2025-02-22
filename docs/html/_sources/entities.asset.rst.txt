Asset
-----------------------------

An asset object represents securities such as stocks or options in Lumibot. Attributes that are tracked for assets are:

* symbol(str): Ticker symbol representing the stock or underlying for options. So for example if trading IBM calls the symbol would just be IBM.
* asset_type(str): Asset type can be either stock, option, future, forex. default: stock
* name(str): Optional to add in the name of the corporation for logging or printout.

**Options Only**

* expiration (str): Expiration of the options contract. Format is datetime.date().
* strike(float): Contract strike price.
* right(str): May enter call or put.
* multiplier(float): Contract multiplier to the underlying. (default: 1)

**Futures Only**

Set up a futures contract using the following:

* symbol(str): Ticker symbol for the contract, > eg: ES
* asset_type(str): "future"
* nexpiration(str): Expiry added as datetime.date() So June 2021 would be datetime.date(2021, 6, 18)`

**Forex Only**

* symbol(str): Currency base: eg: EUR
* currency(str): Conversion currency. eg: GBP
* asset_type(str): "forex"

When creating a new security there are two options.

1. Security symbol: It is permissible to use the security symbol only when trading stocks. Lumibot will convert the ticker symbol to an asset behind the scenes.

2. Asset object: Asset objects may be created at anytime for stocks or options. For options, futures, or forex asset objects are mandatory due to the additional details required to identify and trade these securities.

Assets may be created using the ``Asset()`` method as follows: ``Asset(symbol, asset_type=option, **kwargs)`` * see attributes above.

Examples:

For stocks:

.. code-block:: python

    from lumibot.entities import Asset

    asset = Asset('SPY', asset_type=Asset.AssetType.STOCK)
    order = self.create_order(asset, 10, "buy")
    self.submit_order(order)

For futures:

.. code-block:: python

    from lumibot.entities import Asset

    asset = Asset(
    'ES', asset_type=Asset.AssetType.FUTURE, expiration=datetime.date(2021, 6, 18)
    )
    order = self.create_order(asset, 10, "buy")
    self.submit_order(order)



Documentation
"""""""""""""""""""

.. automodule:: entities.asset
   :members:
   :undoc-members:
   :show-inheritance: