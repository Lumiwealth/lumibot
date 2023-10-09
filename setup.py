import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumibot",
    version="2.8.5",
    author="Robert Grzesik",
    author_email="rob@lumiwealth.com",
    description="Backtesting and Trading Library, Made by Lumiwealth",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumiwealth/lumibot",
    packages=setuptools.find_packages(),
    install_requires=[
        "polygon-api-client",
        "alpaca-py<=0.10.0",
        "alpha_vantage",
        "ibapi==9.81.1.post1",
        "yfinance>=0.2.18",
        "matplotlib>=3.3.3",
        "quandl",
        "pandas>=2.0.0,<=2.1.0",
        "pandas_datareader",
        "pandas_market_calendars>=4.3.1",
        "plotly",
        "flask>=2.2.2",
        "flask-socketio",
        "flask-sqlalchemy",
        "flask-marshmallow",
        "flask-security",
        "marshmallow-sqlalchemy",
        "email_validator",
        "bcrypt",
        "pytest",
        "scipy==1.10.1",  # Newer versions of scipy are currently causing issues
        "ipython",  # required for quantstats, but not in their dependency list for some reason
        "quantstats==0.0.62",
        "python-dotenv",  # Secret Storage
        "ccxt==3.0.61",
        "termcolor",
        "jsonpickle",
        'apscheduler==3.10.4',
        "appdirs",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
