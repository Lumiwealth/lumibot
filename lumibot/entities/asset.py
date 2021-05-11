class Asset:
    """
    This is a base class for Assets.
    Member attributes:
      - symbol (string): The symbol used to retrieve stock quotes if stock. The
          underlying symbol if option.
      - name (string): Long form name. Used only for printing. e.g. `Facebook Inc.`
      - asset_type (string, default: `stock`): One of the following:
        - `stock`
        - `option`


    """

    def __init__(
        self,
        symbol,
        asset_type="stock",
        name="",
        expiration=None,
        strike=None,
        right=None,
        multiplier=None,
    ):
        self.asset_types = ["stock", "option"]

        self.symbol = symbol
        self.asset_type = asset_type
        self.name = name

        # Options
        self.expiration = expiration
        self.strike = strike
        self.right = right
        self.multiplier = multiplier

    @property
    def asset_type(self):
        return self._asset_type

    @asset_type.setter
    def asset_type(self, value):
        if not value:
            value = "stock"
        if value not in self.asset_types:
            raise ValueError(f"Asset asset_type must be one of {self.asset_types}")
        self._asset_type = value

    # Option methods
    def is_option(self):
        return self._asset_type == 'option'


    # Option methods
    def is_option(self):
        return self._asset_type == 'option'


