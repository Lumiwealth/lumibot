"""Small dated records about SEC registrants that SEC's data files do not link in machine-readable form.

Why this file exists
--------------------
* The SEC ticker file (``company_tickers.json``) is today's. When a company reorganizes under
  a new registrant (usually a new holding company), the ticker moves to the new registrant's
  company id (CIK) and the older filings stay under the old one. Neither the ticker file nor
  the submissions or companyfacts files name the predecessor.
* Companies with several share classes file one share count per class. How the classes
  convert into each other is written in the filing text, not in a data field.

Sources
-------
* Predecessors: EDGAR company records. Each successor's first filings on sec.gov (a Form
  8-K12B "notice of succession" where one was filed) name the registrant it replaced. The
  entity names and filing date ranges in the comments below were read from
  ``https://data.sec.gov/api/xbrl/companyfacts/CIK<id>.json`` on 2026-11-14.
* Share class conversion: the company's annual report (Form 10-K), description of its
  common stock.

This is data, not strategy logic. It is incomplete on purpose: add a row when a ticker's
history is missing, with the source in the comment. Company ids are 10 digits, zero padded.
"""

# successor company id -> the company id that filed for the same business before it.
PREDECESSOR_CIK = {
    # ExxonMobil Holdings Corporation (financial data filed from 2026-07-01)
    # <- Exxon Mobil Corporation (filed 2009-08-05 to 2026-05-04).
    "0002115436": "0000034088",
    # BlackRock, Inc. (filed from 2024-11-06) <- BlackRock Finance, Inc., the former
    # BlackRock, Inc. (filed 2009-08-07 to 2024-08-06).
    "0002012383": "0001364742",
    # Alphabet Inc. (filed from 2015-10-29) <- Google Inc. (filed 2009-08-04 to 2015-10-29).
    "0001652044": "0001288776",
    # Broadcom Inc. (filed from 2018-06-14) <- Broadcom Ltd (filed 2016-03-10 to 2018-03-15).
    "0001730168": "0001649338",
    # Broadcom Ltd <- Avago Technologies Ltd (filed 2011-03-10 to 2015-12-17).
    "0001649338": "0001441634",
    # The Walt Disney Company (filed from 2019-05-08) <- the former Walt Disney Co
    # (filed 2009-07-30 to 2019-02-15).
    "0001744489": "0001001039",
}

# Companies whose share classes are not one for one. ``units`` gives each class's size in a
# common unit (keyed by the class member name used on the filing's cover page), ``tickers``
# says which class each ticker is. Classes not listed here are counted one for one.
SHARE_CLASS_UNITS = {
    # Berkshire Hathaway Inc.: each Class A share converts into 1,500 Class B shares
    # (Form 10-K, common stock note). Before the 50-for-1 Class B split on 2010-01-21 the
    # ratio was 30; dates before that are not covered.
    "0001067983": {
        "units": {"CommonClassAMember": 1500.0, "CommonClassBMember": 1.0},
        "tickers": {
            "BRK-A": "CommonClassAMember", "BRK.A": "CommonClassAMember", "BRK A": "CommonClassAMember",
            "BRK-B": "CommonClassBMember", "BRK.B": "CommonClassBMember", "BRK B": "CommonClassBMember",
        },
        "valid_from": "2010-01-21",
    },
}
