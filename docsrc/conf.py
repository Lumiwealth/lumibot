# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

# Since lumibot is installed with pip install -e ., we don't need complex path setups
# Just ensure the current directory is in the path
sys.path.insert(0, os.path.abspath('.'))

# -- Project information -----------------------------------------------------

project = "Lumibot"
copyright = "2021, Lumiwealth"
author = "Lumiwealth Inc."

html_title = "Lumibot Documentation"

source_paths = ["lumibot.brokers", "backtesting"]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": True,
    "light_logo": "Lumibot_Logo.webp",
    "dark_logo": "Lumibot_Logo.webp",
    'announcement': """
    <div class="footer-banner bg-warning text-dark p-3">
        <h5>Need Extra Help?</h5>
        <p>Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_footer_banner" target="_blank" class="text-dark"><strong>Lumiwealth</strong></a> for courses, community, and profitable pre-made trading bots.</p>
    </div>
    """
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_html"]
html_css_files = ["custom.css", "bootstrap/css/bootstrap.css"]

# html_theme_options = {
#     "announcement": """
#     <div class="important-note" style="margin-top: 20px; padding: 20px; background-color: #ffdd57; border-radius: 5px;">
#         <h3>Important!</h3>
#         <p>If you need extra help building your strategies and making them profitable, Lumiwealth has you covered. By visiting Lumiwealth, you not only learn how to use the Lumibot library but also gain access to a wealth of highly profitable algorithms.</p>
#         <p><strong>Our strategies have shown exceptional results, with some achieving over 100% annual returns and others reaching up to 1,000% in backtesting.</strong></p>
#         <p>Join our community of traders, take comprehensive courses, and access our library of profitable trading bots. Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_every_page" target="_blank">Lumiwealth</a> to learn more.</p>
#     </div>
#     """
# }

html_context = {
    'note': """
    <div class="important-note" style="margin-top: 20px; padding: 20px; background-color: #ffdd57; border-radius: 5px;">
        <h3>Important!</h3>
        <p>If you need extra help building your strategies and making them profitable, Lumiwealth has you covered. By visiting Lumiwealth, you not only learn how to use the Lumibot library but also gain access to a wealth of highly profitable algorithms.</p>
        <p><strong>Our strategies have shown exceptional results, with some achieving over 100% annual returns and others reaching up to 1,000% in backtesting.</strong></p>
        <p>Join our community of traders, take comprehensive courses, and access our library of profitable trading bots. Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_every_page" target="_blank">Lumiwealth</a> to learn more.</p>
    </div>
    """
}

# -- Extension configuration ------------------------------------------------

# Update the mocked import list to contain only valid Python identifiers
autodoc_mock_imports = [
    "alpaca", "polygon", "ibapi", "yfinance", "quandl", "sqlalchemy",
    "bcrypt", "scipy", "quantstats", "python_dotenv", "ccxt", "pyarrow", "tqdm",
    "lumiwealth_tradier", "psycopg2_binary", "exchange_calendars", "duckdb", "tabulate",
    "thetadata", "holidays", "psutil", "openai", "schwab_py", "free_proxy", "fp",
]

# Provide light-weight stubs for symbols/modules that moved/changed names
# or to ensure critical attributes like __version__ are strings.
import sys
from types import ModuleType
from unittest.mock import MagicMock

# Custom mock for 'fp' as a package containing an 'fp' submodule
fp_package_mock = MagicMock()
fp_package_mock.__name__ = 'fp'
fp_package_mock.__path__ = ['fp']
fp_package_mock.__file__ = 'fp/__init__.py'
fp_package_mock.__version__ = '0.0.0'
fp_package_mock.__spec__ = MagicMock(name='fp')
sys.modules['fp'] = fp_package_mock

fp_submodule_mock = MagicMock()
fp_submodule_mock.__name__ = 'fp.fp'
fp_submodule_mock.__file__ = 'fp/fp.py'
fp_submodule_mock.__version__ = '0.0.0'
fp_submodule_mock.__spec__ = MagicMock(name='fp.fp')
sys.modules['fp.fp'] = fp_submodule_mock
setattr(fp_package_mock, 'fp', fp_submodule_mock)

# Setup Mocks with string __version__ and __spec__ for all autodoc_mock_imports
for mod_name in autodoc_mock_imports:
    if mod_name not in sys.modules:
        mock_obj = MagicMock()
        mock_obj.__name__ = mod_name
        mock_obj.__file__ = f'{mod_name.replace(".", "/")}.py'
        mock_obj.__version__ = "0.0.0"
        mock_obj.__spec__ = MagicMock(name=mod_name)
        sys.modules[mod_name] = mock_obj

        # Special handling for packages that need to be treated as such with submodules
        if mod_name == "scipy":
            mock_obj.__path__ = [mod_name.replace('.', '/')] 
            stats_mod_name = "scipy.stats"
            _stats_mock = MagicMock(name=stats_mod_name, __version__="0.0.0")
            _stats_mock.__file__ = f'{stats_mod_name.replace(".", "/")}.py'
            _stats_mock.__spec__ = MagicMock(name=stats_mod_name)
            setattr(mock_obj, "stats", _stats_mock)
            sys.modules[stats_mod_name] = _stats_mock
        
        elif mod_name == "sqlalchemy":
            mock_obj.__path__ = [mod_name.replace('.', '/')] 
            exc_mod_name = "sqlalchemy.exc"
            _exc_mock = MagicMock(name=exc_mod_name, __version__="0.0.0")
            _exc_mock.__file__ = f'{exc_mod_name.replace(".", "/")}.py'
            _exc_mock.__spec__ = MagicMock(name=exc_mod_name)
            setattr(mock_obj, "exc", _exc_mock)
            sys.modules[exc_mod_name] = _exc_mock

        elif mod_name == "ibapi":
            mock_obj.__path__ = [mod_name.replace('.', '/')] 
            setattr(mock_obj, "TickerId", MagicMock(name="TickerId", __module__=mod_name))
            setattr(mock_obj, "TickType", MagicMock(name="TickType", __module__=mod_name))
            setattr(mock_obj, "OrderId", MagicMock(name="OrderId", __module__=mod_name))
            setattr(mock_obj, "SetOfString", MagicMock(name="SetOfString", __module__=mod_name))
            setattr(mock_obj, "SetOfFloat", MagicMock(name="SetOfFloat", __module__=mod_name))
            client_mod_name = "ibapi.client"
            _client_mock = MagicMock(name=client_mod_name, __version__="0.0.0")
            _client_mock.__file__ = f'{client_mod_name.replace(".", "/")}.py'
            _client_mock.__spec__ = MagicMock(name=client_mod_name)
            setattr(_client_mock, "TickerId", MagicMock(name="TickerId", __module__=client_mod_name))
            setattr(_client_mock, "TickType", MagicMock(name="TickType", __module__=client_mod_name))
            setattr(_client_mock, "OrderId", MagicMock(name="OrderId", __module__=client_mod_name))
            setattr(_client_mock, "SetOfString", MagicMock(name="SetOfString", __module__=client_mod_name))
            setattr(_client_mock, "SetOfFloat", MagicMock(name="SetOfFloat", __module__=client_mod_name))
            setattr(_client_mock, "EClient", type("EClient", (object,), {"__module__": client_mod_name}))
            setattr(mock_obj, "client", _client_mock)
            sys.modules[client_mod_name] = _client_mock
            wrapper_mod_name = "ibapi.wrapper"
            _wrapper_mock = MagicMock(name=wrapper_mod_name, __version__="0.0.0")
            _wrapper_mock.__file__ = f'{wrapper_mod_name.replace(".", "/")}.py'
            _wrapper_mock.__spec__ = MagicMock(name=wrapper_mod_name)
            setattr(_wrapper_mock, "EWrapper", type("EWrapper", (object,), {"__module__": wrapper_mod_name}))
            common_ibapi_classes = ["ebar", "ComissãoRelatório", "Contract", "ContractDetails", "DeltaNeutralContract", "Execution", "ExecutionFilter", "Order", "OrderComboLeg", "OrderState", "SoftDollarTier", "TagValue", "HistogramData", "NewsProvider", "PriceIncrement", "RealTimeBar", "ScannerSubscription", "TickAttrib", "TickAttribBidAsk", "TickAttribLast", "TickAttribMana", "TickAttribRtShortable", "FamilyCode", "MarketDataType", "SmartComponent", "FaDataType", "MarketDepthData", "HistoricalTick", "HistoricalTickBidAsk", "HistoricalTickLast", "TickData", "HistoricalSchedule", "PnL", "PnLSingle", "AccountValue", "NewsArticle", "HistoricalNews", "NewsTick", "NewsBulletin"]
            if "EClient" in common_ibapi_classes: common_ibapi_classes.remove("EClient")
            for class_name in common_ibapi_classes:
                if class_name == "EWrapper": continue
                setattr(_wrapper_mock, class_name, MagicMock(name=class_name, __module__=wrapper_mod_name))
            setattr(mock_obj, "wrapper", _wrapper_mock)
            sys.modules[wrapper_mod_name] = _wrapper_mock
            common_mod_name = "ibapi.common"
            _common_mock = MagicMock(name=common_mod_name, __version__="0.0.0")
            _common_mock.__file__ = f'{common_mod_name.replace(".", "/")}.py'
            _common_mock.__spec__ = MagicMock(name=common_mod_name)
            setattr(_common_mock, "TickerId", MagicMock(name="TickerId", __module__=common_mod_name))
            setattr(_common_mock, "OrderId", MagicMock(name="OrderId", __module__=common_mod_name))
            setattr(_common_mock, "SetOfString", MagicMock(name="SetOfString", __module__=common_mod_name))
            setattr(_common_mock, "SetOfFloat", MagicMock(name="SetOfFloat", __module__=common_mod_name))
            common_ibapi_common_types = ["BarData", "RealTimeBar", "TickAttrib", "TickAttribBidAsk", "TickAttribLast", "HistoricalTick", "HistoricalTickBidAsk", "HistoricalTickLast", "TagValue", "NewsTick", "PriceIncrement", "SmartComponent", "HistogramData", "NewsProvider", "Execution", "CommissionReport", "OrderState"]
            for type_name in common_ibapi_common_types:
                 if not hasattr(_common_mock, type_name):
                    setattr(_common_mock, type_name, MagicMock(name=type_name, __module__=common_mod_name))
            setattr(mock_obj, "common", _common_mock)
            sys.modules[common_mod_name] = _common_mock
            ticktype_mod_name = "ibapi.ticktype"
            _ticktype_mock = MagicMock(name=ticktype_mod_name, __version__="0.0.0")
            _ticktype_mock.__file__ = f'{ticktype_mod_name.replace(".", "/")}.py'
            _ticktype_mock.__spec__ = MagicMock(name=ticktype_mod_name)
            setattr(_ticktype_mock, "TickType", MagicMock(name="TickType", __module__=ticktype_mod_name))
            setattr(mock_obj, "ticktype", _ticktype_mock)
            sys.modules[ticktype_mod_name] = _ticktype_mock
            contract_mod_name = "ibapi.contract"
            _contract_mock = MagicMock(name=contract_mod_name, __version__="0.0.0")
            _contract_mock.__file__ = f'{contract_mod_name.replace(".", "/")}.py'
            _contract_mock.__spec__ = MagicMock(name=contract_mod_name)
            setattr(_contract_mock, "SetOfString", MagicMock(name="SetOfString", __module__=contract_mod_name))
            setattr(_contract_mock, "SetOfFloat", MagicMock(name="SetOfFloat", __module__=contract_mod_name))
            setattr(mock_obj, "contract", _contract_mock)
            sys.modules[contract_mod_name] = _contract_mock
        
        elif mod_name == "lumiwealth_tradier":
            mock_obj.__path__ = [mod_name.replace('.', '/')] # Mark as package
            base_mod_name = "lumiwealth_tradier.base"
            _base_mock = MagicMock(name=base_mod_name, __version__="0.0.0")
            _base_mock.__file__ = f'{base_mod_name.replace(".", "/")}.py'
            _base_mock.__spec__ = MagicMock(name=base_mod_name)
            setattr(mock_obj, "base", _base_mock)
            sys.modules[base_mod_name] = _base_mock
        
        elif mod_name == "schwab_py":
            sys.modules['schwab'] = mock_obj 
            mock_obj.__path__ = [mod_name.replace('.', '/')] 
            mock_obj.__file__ = f'{mod_name.replace(".", "/")}/__init__.py'
            auth_mod_name = "schwab.auth"
            _auth_mock = MagicMock(name=auth_mod_name, __version__="0.0.0")
            _auth_mock.__file__ = f'{auth_mod_name.replace(".", "/")}.py'
            _auth_mock.__spec__ = MagicMock(name=auth_mod_name)
            setattr(mock_obj, "auth", _auth_mock)
            sys.modules[auth_mod_name] = _auth_mock
            client_mod_name_schwab = "schwab.client"
            _client_mock_schwab = MagicMock(name=client_mod_name_schwab, __version__="0.0.0")
            _client_mock_schwab.__file__ = f'{client_mod_name_schwab.replace(".", "/")}.py'
            _client_mock_schwab.__spec__ = MagicMock(name=client_mod_name_schwab)
            setattr(mock_obj, "client", _client_mock_schwab)
            sys.modules[client_mod_name_schwab] = _client_mock_schwab
            streaming_mod_name_schwab = "schwab.streaming"
            _streaming_mock_schwab = MagicMock(name=streaming_mod_name_schwab, __version__="0.0.0")
            _streaming_mock_schwab.__file__ = f'{streaming_mod_name_schwab.replace(".", "/")}.py'
            _streaming_mock_schwab.__spec__ = MagicMock(name=streaming_mod_name_schwab)
            setattr(mock_obj, "streaming", _streaming_mock_schwab) 
            sys.modules[streaming_mod_name_schwab] = _streaming_mock_schwab
        
        elif mod_name == "polygon":
            mock_obj.__path__ = [mod_name.replace('.', '/')] # Mark as package
            exceptions_mod_name = "polygon.exceptions"
            _exceptions_mock = MagicMock(name=exceptions_mod_name, __version__="0.0.0")
            _exceptions_mock.__file__ = f'{exceptions_mod_name.replace(".", "/")}.py'
            _exceptions_mock.__spec__ = MagicMock(name=exceptions_mod_name)
            setattr(mock_obj, "exceptions", _exceptions_mock)
            sys.modules[exceptions_mod_name] = _exceptions_mock

# Custom/detailed mocking for alpaca and its submodules
alpaca_main_pkg_name = "alpaca"
alpaca_mock = sys.modules.get(alpaca_main_pkg_name)
if not isinstance(alpaca_mock, MagicMock) or not hasattr(alpaca_mock, '__path__'):
    alpaca_mock = MagicMock(name=alpaca_main_pkg_name, __version__='0.0.0')
    alpaca_mock.__path__ = [alpaca_main_pkg_name.replace('.', '/')]
    alpaca_mock.__file__ = f'{alpaca_main_pkg_name.replace(".", "/")}/__init__.py'
    alpaca_mock.__spec__ = MagicMock(name=alpaca_main_pkg_name)
    sys.modules[alpaca_main_pkg_name] = alpaca_mock

data_pkg_name = "alpaca.data"
data_mock = getattr(alpaca_mock, 'data', None)
if not isinstance(data_mock, MagicMock) or not hasattr(data_mock, '__path__'):
    data_mock = MagicMock(name=data_pkg_name, __version__='0.0.0')
    data_mock.__path__ = [data_pkg_name.replace('.', '/')]
    data_mock.__file__ = f'{data_pkg_name.replace(".", "/")}/__init__.py'
    data_mock.__spec__ = MagicMock(name=data_pkg_name)
    setattr(alpaca_mock, 'data', data_mock)
    sys.modules[data_pkg_name] = data_mock

historical_pkg_name = "alpaca.data.historical"
historical_mock = getattr(data_mock, 'historical', None)
if not isinstance(historical_mock, MagicMock) or not hasattr(historical_mock, '__path__'):
    historical_mock = MagicMock(name=historical_pkg_name, __version__='0.0.0')
    historical_mock.__path__ = [historical_pkg_name.replace('.', '/')]
    historical_mock.__file__ = f'{historical_pkg_name.replace(".", "/")}/__init__.py'
    historical_mock.__spec__ = MagicMock(name=historical_pkg_name)
    setattr(data_mock, 'historical', historical_mock)
    sys.modules[historical_pkg_name] = historical_mock

for _sym in ("OptionHistoricalDataClient", "CryptoHistoricalDataClient", "StockHistoricalDataClient"):
    if not hasattr(historical_mock, _sym):
        mock_class = MagicMock(name=_sym, __module__=historical_pkg_name)
        setattr(historical_mock, _sym, mock_class)

requests_mod_name = "alpaca.data.requests"
requests_mock = getattr(data_mock, 'requests', None)
if not isinstance(requests_mock, MagicMock):
    requests_mock = MagicMock(name=requests_mod_name, __version__='0.0.0')
    requests_mock.__file__ = f'{requests_mod_name.replace(".", "/")}.py'
    requests_mock.__spec__ = MagicMock(name=requests_mod_name)
    setattr(data_mock, 'requests', requests_mock)
    sys.modules[requests_mod_name] = requests_mock

trading_mod_name = "alpaca.trading"
trading_mock = getattr(alpaca_mock, 'trading', None)
if not isinstance(trading_mock, MagicMock):
    trading_mock = MagicMock(name=trading_mod_name, __version__='0.0.0')
    trading_mock.__file__ = f'{trading_mod_name.replace(".", "/")}.py'
    trading_mock.__spec__ = MagicMock(name=trading_mod_name)
    setattr(alpaca_mock, 'trading', trading_mock)
    sys.modules[trading_mod_name] = trading_mock

# ---------------------------------------------------------------------------