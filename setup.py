import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumibot",
    version="3.17.14",
    author="Robert Grzesik",
    author_email="rob@lumiwealth.com",
    description="Backtesting and Trading Library, Made by Lumiwealth",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumiwealth/lumibot",
    packages=setuptools.find_packages(),
    license="MIT",  # Add license argument
    include_package_data=True, 
    install_requires=[
        "polygon-api-client>=1.13.3",
        "alpaca-py>=0.28.1",
        "alpha_vantage",
        "ibapi==9.81.1.post1",
        "yfinance>=0.2.61",
        "matplotlib>=3.3.3",
        "quandl",
        # NumPy 1.20.0+ supports modern features, 2.0+ adds compatibility for latest ecosystem
        "numpy>=1.20.0",
        "pandas>=2.2.0",
        "pandas_market_calendars>=5.1.0",
        "plotly>=5.18.0",
        "sqlalchemy",
        "bcrypt",
        "pytest",
        # SciPy 1.14.0+ supports NumPy 2.x
        "scipy>=1.14.0",
        "quantstats-lumi>=1.0.1",
        "python-dotenv",  # Secret Storage
        "ccxt>=4.4.80",
        "termcolor",
        "jsonpickle",
        "apscheduler>=3.10.4",
        "appdirs",
        # PyArrow 15.0.0+ supports NumPy 2.x
        "pyarrow>=15.0.0",
        "tqdm",
        "lumiwealth-tradier>=0.1.17",
        "pytz",
        "psycopg2-binary",
        # Exchange calendars 4.6.0+ supports NumPy 2.x
        "exchange_calendars>=4.6.0",
        "duckdb",
        "tabulate",
        "thetadata==0.9.11",
        "databento>=0.42.0",
        "holidays",
        "psutil",
        "openai",
        "schwab-py>=1.5.0",
        "Flask>=2.3",
        "free-proxy",
        "requests-oauthlib",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)