:seo_title: CoinEx adapter status in LumiBot

CoinEx adapter status
=====================

.. meta::
   :description: CCXT removed its CoinEx adapter. Check installed adapter availability and find current LumiBot crypto exchange guides.

CCXT removed its ``coinex`` adapter in its upstream
`delisting change <https://github.com/ccxt/ccxt/pull/30737>`_.
Current CCXT installations do not provide this adapter, so this page no longer
offers connection or trading examples. The page stays at its existing URL for
users following older links.

.. include:: _snippets/botspot_mcp_callout.rst

Frequently asked questions
--------------------------

Why is the CoinEx setup guide no longer here?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CCXT removed the adapter from its distributed package. The preserved CCXT
4.5.64 metadata describes an older installation. Instructions to install LumiBot and select
``coinex`` would no longer work with current CCXT.

Can a current CCXT installation connect to CoinEx through LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. LumiBot's shared crypto broker needs an adapter supplied by the installed
CCXT package. The upstream removal does not establish whether CoinEx's own
services remain available; it means this CCXT connection is no longer provided.

Does this documentation change disable an older installed adapter?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. LumiBot still discovers the adapters in the installed CCXT package and has
no exchange allowlist. An older installation that includes ``coinex`` keeps
that runtime path. Adapter presence alone does not verify that the exchange's
current API accepts its requests.

The `earlier guide <https://github.com/Lumiwealth/lumibot/blob/71c21d231ee9a64170458b087a8076a24ead95f1/docsrc/brokers.ccxt.coinex.rst>`_
remains available as historical documentation for those installations.

How do I check which adapters my installation provides?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This reads installed package metadata without connecting to an exchange:

.. code-block:: python

   import ccxt

   print(ccxt.__version__)
   print("coinex" in ccxt.exchanges)
   print(ccxt.exchanges)

Where can I find other crypto exchange guides?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :doc:`brokers.crypto` for exchange guides and :doc:`brokers.ccxt` for the
shared broker configuration. Each guide describes its exchange's credentials
and supported market types.

Sources
-------

* `CCXT CoinEx delisting change <https://github.com/ccxt/ccxt/pull/30737>`_
  (reviewed 2026-11-01).
* The `CCXT 4.5.64 facts snapshot <https://github.com/Lumiwealth/lumibot/blob/71c21d231ee9a64170458b087a8076a24ead95f1/docsrc/_data/ccxt_exchange_facts.json>`_
  was generated 2026-10-23. It is historical metadata, not a current availability claim.
