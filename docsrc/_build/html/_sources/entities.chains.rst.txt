Chains Entity
=============

``Chains`` represents the complete option-chain snapshot returned by
``Strategy.get_chains()``.  It behaves like a mapping but adds convenience
helpers so you can focus on trading logic instead of dictionary plumbing.

At a glance
-----------

.. code-block:: python

    spy_chains = self.get_chains(Asset("SPY"))  # -> Chains

    # Quick introspection
    print(spy_chains)          # <Chains exchange=SMART multiplier=100 expirations=30 calls=5000 puts=5000>

    # Basic data access -------------------------------------------------
    expiries   = spy_chains.expirations()           # list[str] of CALL expirations
    put_dates  = spy_chains.expirations("PUT")      # list[str] of PUT expirations
    strikes_atm = spy_chains.strikes(expiries[0])   # strikes list for first expiry, CALL side by default

    # Calculate an at-the-money strike
    underlying_px = self.get_last_price("SPY")
    atm = min(strikes_atm, key=lambda s: abs(s - underlying_px))

    # Pick the matching PUT strike for a credit spread
    put_strikes = spy_chains.strikes(expiries[0], "PUT")
    otm_put = max(s for s in put_strikes if s < underlying_px)

    # Build an Asset object
    contract = Asset(
        symbol="SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=expiries[0],
        strike=otm_put,
        right=Asset.OptionRight.PUT,
    )

Design notes
------------

* ``Chains`` **inherits from** ``dict`` so any existing code that used the raw
  mapping (``chains["Chains"]["PUT"]`` …) keeps working.
* All helper methods return lightweight Python types – no Pandas dependency.
* Attribute summary:

  ========  ===============================================================
  Property  Description
  ========  ===============================================================
  ``calls``          Mapping ``{expiry: [strike, …]}`` for CALLs
  ``puts``           Mapping ``{expiry: [strike, …]}`` for PUTs
  ``multiplier``     Contract multiplier (typically 100)
  ``exchange``       Primary routing exchange returned by the API
  ========  ===============================================================

API reference
-------------

.. autoclass:: lumibot.entities.chains.Chains
    :members:
    :undoc-members:
    :show-inheritance: 