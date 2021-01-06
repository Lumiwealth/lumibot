class Bars:
    def __init__(self, df, raw=None):
        """
        df columns: close, dividend, volume, momentum
        df index: pd.Timestamp
        """
        self.df = df
        self._raw = raw

    def __repr__(self):
        return repr(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()

    def get_last_price(self):
        return self.df["close"][-1]

    def get_last_dividend(self):
        return self.df["dividend"][-1]

    def get_momentum_df(self, momentum_length):
        df = self.df.copy()
        df["momentum"] = df["close"].pct_change(periods=momentum_length)
        return df[df["momentum"].notna()]

    def get_momentum(self):
        n_rows = self.df.shape[0]
        momentum = self.df["close"].pct_change(n_rows - 1)[-1]
        return momentum

    def get_total_volume(self, period=None):
        if period is None:
            volume = self.df["volume"].sum()
        else:
            volume = self.df[-period:]["volume"].sum()

        return volume
