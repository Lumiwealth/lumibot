import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumibot",
    version="3.9.5",
    author="Robert Grzesik",
    author_email="rob@lumiwealth.com",
    description="Backtesting and Trading Library, Made by Lumiwealth",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumiwealth/lumibot",
    packages=setuptools.find_packages(),
    install_requires=[
        "polygon-api-client>=1.13.3",
        "alpaca-py>=0.28.1",
        "alpha_vantage",
        "ibapi==9.81.1.post1",
        "yfinance>=0.2.46",
        "matplotlib>=3.3.3",
        "quandl",
        # Numpy greater than 1.20.0 and less than 2 because v2 has compatibility issues with a few libraries
        "numpy>=1.20.0, <2",
        "pandas>=2.2.0",
        "pandas_market_calendars>=4.3.1",
        "plotly>=5.18.0",
        "sqlalchemy",
        "bcrypt",
        "pytest",
        "scipy>=1.13.0",
        "quantstats-lumi>=0.3.3",
        "python-dotenv",  # Secret Storage
        "ccxt>=4.3.74",
        "termcolor",
        "jsonpickle",
        "apscheduler>=3.10.4",
        "appdirs",
        "pyarrow",
        "tqdm",
        "lumiwealth-tradier>=0.1.14",
        "pytz",
        "psycopg2-binary",
        "exchange_calendars>=4.5.2",
        "duckdb",
        "tabulate",
        "thetadata",
        "holidays",
        "psutil",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)