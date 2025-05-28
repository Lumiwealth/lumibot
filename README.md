[![CI Status](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml/badge.svg?branch=dev)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![Coverage](https://raw.githubusercontent.com/Lumiwealth/lumibot/badge/coverage.svg)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)

# Lumibot - A Backtesting and Trading Library for Stocks, Options, Crypto, Futures, FOREX and More!

Lumibot is a backtesting and trading library for stocks, options, crypto, futures and more. It is made so that the same code you use for backtesting can be used for live trading, making it easy to transition from backtesting to live trading. Lumibot is a highly flexible library that allows you to create your own strategies and indicators, and backtest them on historical data. It is also highly optimized for speed, so you can backtest your strategies quickly and efficiently.

**IMPORTANT: This library requires data for backtesting. The recommended data source is [Polygon.io](https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10) (a free tier is available too). Please click the link to give us credit for the sale, it helps support this project. You can use the coupon code 'LUMI10' for 10% off.**

## Documentation - 👇 Start Here 👇

To get started with Lumibot, you can check out our documentation below.

**Check out the documentation for the project here: 👉 <http://lumibot.lumiwealth.com/> 👈**

## Build Trading Bots with AI

Want to build trading bots without code? Check out our new platform [BotSpot](https://botspot.trade/sales?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme) where you can create and deploy trading strategies using AI! BotSpot allows you to:

- Build trading bots using natural language and AI
- Test your strategies with historical data
- Deploy your bots to trade automatically
- Join a community of algorithmic traders

**Visit [BotSpot.trade](https://botspot.trade/sales?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme) to get started building AI-powered trading bots today!**

## Blog

Our blog has lots of example strategies and shows you how to run a bot using LumiBot. Check the blog out here:

**https://lumiwealth.com/blog/**

## Run a backtest

To run a backtest, you can use the following code snippet:

```bash
python -m lumibot.example_strategies.stock_buy_and_hold
```

## Run an Example Strategy

We made a small example strategy to show you how to use Lumibot in this GitHub repository: [Example Algorithm GitHub](https://github.com/Lumiwealth-Strategies/stock_example_algo)

To run this example strategy, click on the `Deploy to Render` button below to deploy the strategy to Render (our recommendation). You can also run the strategy on Repl.it by clicking on the `Run on Repl.it` button below.

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/Lumiwealth-Strategies/stock_example_algo)

[![Run on Repl.it](https://replit.com/badge/github/Lumiwealth-Strategies/stock_example_algo)](https://replit.com/new/github/Lumiwealth-Strategies/stock_example_algo)

**For more information on this example strategy, you can check out the README in the example strategy repository here: [Example Algorithm](https://github.com/Lumiwealth-Strategies/stock_example_algo)**

## Contributors

If you want to contribute to Lumibot, you can check how to get started below. We are always looking for contributors to help us out!

Here's a video to help you get started with contributing to Lumibot: [Watch The Video](https://youtu.be/Huz6VxqafZs)

**Steps to contribute:**

0. Watch the video: [Watch The Video](https://youtu.be/Huz6VxqafZs)
1. Clone the repository to your local machine
2. Create a new branch for your feature
3. Run `pip install -r requirements_dev.txt` to install the developer dependencies
4. Install all the requirements from setup.py: `pip install -e .`
5. Make your changes
6. Run `pytest` to make sure all the tests pass
7. Create a pull request to merge your branch into master

## Running Tests

We use pytest for our testing framework. Some tests require API keys to be in a `.env` file in the root directory. To run the tests, you can run the following command:

```bash
pytest
```

To run an individual test file, you can run the following command:

```bash
pytest tests/test_asset.py
```

### Showing Code Coverage

To show code coverage, you can run the following command:

```bash
coverage run; coverage report; coverage html
```

#### Adding an Alias on Linux or MacOS

This will show you the code coverage in the terminal and also create a folder called "htmlcov" which will have a file called "index.html". You can open this file in your browser to see the code coverage in a more readable format.

If you don't want to keep typing out the command, you can add it as an alias in bash. To do this, you can run the following command:

```bash
alias cover='coverage run; coverage report; coverage html'
```

This will now allow you to run the command by just typing "cover" in the terminal.

```bash
cover
```

If you want to also add it to your .bashrc file. You can do this by running the following command:

```bash
echo "alias cover='coverage run; coverage report; coverage html'" >> ~/.bashrc
```

#### Adding an Alias on Windows

If you are on Windows, you can add an alias by running the following command:

Add to your PowerShell Profile: (profile.ps1)

```powershell
function cover { 
 coverage run
 coverage report
 coverage html
}
```

### Setting Up PyTest in VS Code

To set up in VS Code for debugging, you can add the following to your launch.json file under "configurations". This will allow you to go into "Run and Debug" and run the tests from there, with breakpoints and everything.

NOTE: You may need to change args to the path of your tests folder.

```json
{
    "name": "Python: Pytest",
    "type": "python",
    "request": "launch",
    "module": "pytest",
    "args": [
        "lumibot/tests"
    ],
    "console": "integratedTerminal",
}
```

Here's an example of an actual launch.json file:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Pytest",
            "type": "python",
            "request": "launch",
            "module": "pytest",
            "args": [
                "lumibot/tests"
            ],
            "console": "integratedTerminal",
        }
    ]
}
```

## Notes on data sources

This table points out some of the differences between the data sources we use in Lumibot. These refer to the data 
returned in a Bars entity that is returned from calls to get_historical_prices. 

| data_source | type  | OHLCV | split adjusted | dividends | returns | dividend adjusted returns |
|-------------|-------|-------|----------------|-----------|---------|---------------------------|
| yahoo       | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |
| alpaca      | stock | Yes   | Yes            | No        | Yes     | No                        |
| polygon     | stock | Yes   | Yes            | No        | Yes     | No                        |
| Tradier     | stock | Yes   | Yes            | No        | Yes     | No                        |
| Pandas*     | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |

*Pandas is not a data source per se, but it can load csv files in the same format as Yahoo dataframes,
which can contain dividends.

## An assortment of git commands our contributors may find useful

Making a new branch and pulling from main:
```shell
git checkout -b my-feature
git fetch origin
git merge origin/dev
```
Committing work to you feature branch:
```shell
git add .
git commit -m "my changes"
git push -u origin my-feature
```

If work on main progressed while you were in another branch, this is how you rebase it into your branch. Note that
since you've rebased your local branch, you'll need to force push your changes to update the remote branch. 
The --force-with-lease option is a safer alternative to --force as it will abort the push if there are any new 
commits on the remote that you haven't incorporated into your local branch
```shell
git checkout dev
git fetch origin
git merge origin/dev
git checkout my-feature
git rebase dev
git checkout my-feature
git push --force-with-lease origin my-feature
```

When ready to merge the branch into main, go into github, create a pull request, and await review. When your PR is approved it will automatically be merged into the dev branch remotely. Now, you can delete your local branch and the remote branch.
```shell
git checkout dev
git fetch origin
git merge origin/dev
git branch -D my-feature
git push origin --delete my-feature
```

## Community

If you want to learn more about Lumibot or Algorithmic Trading then you will love out communities! You can join us on Discord.

**Join us on Discord: <https://discord.gg/TmMsJCKY3T>**

**Build AI-powered trading bots on [BotSpot.trade](https://botspot.trade/)** - Our platform for creating, testing, and deploying trading strategies with AI assistance!

## Courses

If you need extra help building your algorithm, we have courses to help you out.

**For our Algorithmic Trading course: <https://lumiwealth.com/algorithmic-trading-landing-page>**

**For our Machine Learning for Trading course: <https://www.lumiwealth.com/product-category/machine-learning-purchase/>**

**For our Options Trading course: <https://www.lumiwealth.com/product-category/options-trading-purchase/>**

**Looking for a no-code solution? Build trading bots with AI on [BotSpot](https://botspot.trade/)**

## License

This library is covered by the MIT license for open sourced software which can be found here: <https://github.com/Lumiwealth/lumibot/blob/master/LICENSE>
