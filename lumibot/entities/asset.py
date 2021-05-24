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
      If asset_type is `option` then the following fields are mandatory.
      - expiration (string, "YYYY-MM-DD"): Contract expiration date.
      - strike (float): Contract strike price.
      - right(string): `call` or `put`
      - multiplier (int): Contract leverage over the underlying.
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

    def __repr__(self):
        stock_repr = f"{self.symbol.upper()}, Type: {self.asset_type} "
        option_repr = (
            f"Exp: {self.expiration} "
            f"Strike: {self.strike} "
            f"Right: {self.right} "
        )

        if self.asset_type == 'stock':
            return stock_repr
        else:
            return stock_repr + option_repr



