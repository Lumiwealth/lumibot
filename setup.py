import json

import requests
import setuptools


def increment_version():
    resp = requests.get("https://pypi.python.org/pypi/lumibot/json")
    j = json.loads(resp.content)
    last_version = j["info"]["version"]
    version_numbers = last_version.split(".")
    version_numbers[-1] = str(int(version_numbers[-1]) + 1)
    new_version = ".".join(version_numbers)
    return new_version


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumibot",
    version="2.6.0",
    author="Robert Grzesik",
    author_email="rob@lumiwealth.com",
    description="Backtesting and Trading Library, Made by Lumiwealth",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumiwealth/lumibot",
    packages=setuptools.find_packages(),
    install_requires=[
        "pydantic",
        "alpaca_trade_api==2.3.0",
        "alpha_vantage",
        "ibapi==9.81.1.post1",
        "yfinance>=0.2.18",
        "matplotlib>=3.3.3",
        "quandl",
        "pandas>=1.4.0,<2.0.0",  # pandas v2 currently causing issues with quant stats (v0.0.59)
        "pandas_datareader",
        "pandas_market_calendars>=4.1.2",
        "plotly",
        "flask-socketio",
        "flask-sqlalchemy",
        "flask-marshmallow",
        "marshmallow-sqlalchemy",
        "flask-security",
        "email_validator",
        "bcrypt",
        "pytest",
        "scipy",
        "quantstats==0.0.59",
        "ccxt==3.0.61",
        "termcolor",
        "jsonpickle",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
