import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumibot",
    version="3.14.5",
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
        # Numpy greater than 1.20.0 and less than 2 because v2 has compatibility issues with a few libraries
        "numpy>=1.20.0, <2",
        "pandas>=2.2.0",
        "pandas_market_calendars>=5.1.0",
        "plotly>=5.18.0",
        "sqlalchemy",
        "bcrypt",
        "pytest",
        "scipy>=1.13.0",
        "quantstats-lumi>=1.0.1",
        "python-dotenv",  # Secret Storage
        "ccxt>=4.4.80",
        "termcolor",
        "jsonpickle",
        "apscheduler>=3.10.4",
        "appdirs",
        "pyarrow",
        "tqdm",
        "lumiwealth-tradier>=0.1.16",
        "pytz",
        "psycopg2-binary",
        "exchange_calendars>=4.5.2",
        "duckdb",
        "tabulate",
        "thetadata==0.9.11",
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