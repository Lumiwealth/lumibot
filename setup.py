import shutil
from pathlib import Path

import setuptools
from setuptools.command.build_py import build_py as _build_py

PROJECT_ROOT = Path(__file__).resolve().parent
DIST_DIR = PROJECT_ROOT / "dist"
if DIST_DIR.exists():
    shutil.rmtree(DIST_DIR)


class BuildWithThetaJar(_build_py):
    """Optionally bundle ThetaTerminal.jar if present locally.

    This makes ThetaData optional at build/install time. If the JAR is not
    present in lumibot/resources, we simply skip bundling it instead of failing
    the build.
    """

    def run(self):
        build_lib = Path(self.build_lib)
        if build_lib.exists():
            shutil.rmtree(build_lib)
        super().run()
        self._maybe_copy_theta_terminal()

    def _maybe_copy_theta_terminal(self):
        src = PROJECT_ROOT / "lumibot" / "resources" / "ThetaTerminal.jar"
        if not src.exists():
            # Optional: nothing to do if JAR isn't in the repo
            print("[build] ThetaTerminal.jar not found, skipping bundling (ThetaData is optional).")
            return
        dest = Path(self.build_lib) / "lumibot" / "resources" / "ThetaTerminal.jar"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        print(
            f"[build] Bundled ThetaTerminal.jar -> {dest} "
            f"(size={dest.stat().st_size} bytes)"
        )

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

theta_jar_path = PROJECT_ROOT / "lumibot" / "resources" / "ThetaTerminal.jar"

setuptools.setup(
    name="lumibot",
    version="4.5.91",
    author="Robert Grzesik",
    author_email="rob@botspot.trade",
    description="Python framework for algorithmic trading: backtesting and live deployment for stocks, options, crypto, futures, and forex. Same code for backtest and live trading.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumiwealth/lumibot",
    packages=setuptools.find_packages(include=["lumibot", "lumibot.*"]),
    license="MIT",  # Add license argument
    include_package_data=True,
    install_requires=[
        "polygon-api-client>=1.13.3",
        "alpaca-py>=0.42.0",
        "alpha_vantage",
        "ibapi==9.81.1.post1",
        "yfinance>=0.2.61",
        "matplotlib>=3.3.3",
        "quandl",
        # NumPy 2.5 emits noisy timedelta deprecation warnings through pandas 2.x calendar paths.
        "numpy>=1.20.0,<2.5.0",
        "pandas>=2.2.0",
        "polars>=1.32.3",
        "pandas_market_calendars>=5.1.0",
        "pandas-ta-classic>=0.3.14b0",
        "plotly>=5.18.0",
        "sqlalchemy",
        "bcrypt",
        "pytest",
        "yappi>=1.6.0",
        # SciPy 1.14.0+ supports NumPy 2.x
        "scipy>=1.14.0",
        "quantstats-lumi>=1.1.5,<1.2.0",
        "python-dotenv",  # Secret Storage
        "ccxt>=4.5.50",  # 4.5.50+ includes WEEX exchange support
        "termcolor",
        "jsonpickle",
        "apscheduler>=3.10.4",
        "appdirs",
        # PyArrow 15.0.0+ supports NumPy 2.x
        "pyarrow>=15.0.0",
        "tqdm",
        "lumiwealth-tradier>=0.1.18",
        "py-clob-client-v2>=1.0.1",
        "pytz",
        "psycopg2-binary",
        # Exchange calendars 4.6.0+ supports NumPy 2.x
        "exchange_calendars>=4.6.0",
        "duckdb",
        "tabulate",
        "databento>=0.42.0",
        "holidays",
        "psutil",
        "openai",
        "setuptools<81",
        "google-adk[extensions]>=2.1.0,<3.0.0",
        "google-genai>=1.72.0,<2.0.0",
        "litellm>=1.83.7,<=1.83.14",
        "anyio>=4.10.0",
        "mcp>=1.26.0,<2",
        "schwab-py>=1.5.0",
        "Flask>=2.3",
        "free-proxy",
        "requests-oauthlib",
        "boto3>=1.40.64",
        "httpx",
    ],
    # Include configuration files, and only include ThetaTerminal.jar if present
    package_data={
        "lumibot": [
            "resources/conf.yaml",
            "components/agents/skills/*/SKILL.md",
            "components/agents/skills/*/agents/*.yaml",
            "components/agents/skills/*/references/*.md",
            "example_strategies/agent_rules/*.json",
        ] + (["resources/ThetaTerminal.jar"] if theta_jar_path.exists() else []),
    },
    extras_require={
        # Optional dependencies to enable ThetaData support
        "thetadata": [
            "thetadata",
        ],
    },
    keywords=[
        "algorithmic-trading",
        "backtesting",
        "trading-bot",
        "live-trading",
        "stocks",
        "options",
        "crypto",
        "cryptocurrency",
        "futures",
        "forex",
        "quantitative-finance",
        "alpaca",
        "interactive-brokers",
        "tradier",
        "polymarket",
        "schwab",
        "trading-strategies",
        "paper-trading",
        "ai-trading",
        "multi-asset",
        "event-driven",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    project_urls={
        "Documentation": "https://lumibot.lumiwealth.com/",
        "Bug Tracker": "https://github.com/Lumiwealth/lumibot/issues",
        "Source Code": "https://github.com/Lumiwealth/lumibot",
        "Reddit Community": "https://www.reddit.com/r/BotSpotTrade/",
        "Discord Community": "https://discord.gg/4R9j6T3PN8",
        "BotSpot Platform": "https://botspot.trade/",
    },
    python_requires=">=3.10",
    cmdclass={"build_py": BuildWithThetaJar},
)
