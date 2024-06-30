# Lumibot - A Backtesting and Trading Library for Stocks, Options, Crypto, Futures and More!

Lumibot is a backtesting and trading library for stocks, options, crypto, futures and more. It is made so that the same code you use for backtesting can be used for live trading, making it easy to transition from backtesting to live trading. Lumibot is a highly flexible library that allows you to create your own strategies and indicators, and backtest them on historical data. It is also highly optimized for speed, so you can backtest your strategies quickly and efficiently.

**IMPORTANT: This library requires data for backtesting. The recommended data source is Polygon, and you can get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 (a free tier is available too) Please use the full link to give us credit for the sale, it helps support this project. You can use the coupon code 'LUMI10' for 10% off.**

## Documentation - 👇 Start Here 👇

To get started with Lumibot, you can check out our documentation below.

**Check out the documentation for the project here: 👉 <http://lumibot.lumiwealth.com/> 👈**

## Contributors

If you want to contribute to Lumibot, you can check how to get started below. We are always looking for contributors to help us out!

**Steps to contribute:**

1. Clone the repository to your local machine
2. Create a new branch for your feature
3. Run `pip install -r requirements_dev.txt` to install the developer dependencies
4. Install all the requriements from setup.py: `pip install -e .`
5. Make your changes
6. Run `pytest` to make sure all the tests pass
7. Create a pull request to merge your branch into master

## Running Tests

We use pytest for our testing framework. To run the tests, you can run the following command:

```bash
pytest
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

## Community

If you want to learn more about Lumibot or Algorithmic Trading then you will love out communities! You can join us on Discord.

**Join us on Discord: <https://discord.gg/TmMsJCKY3T>**

## Courses

If you need extra help building your algorithm, we have courses to help you out.

**For our Algorithmic Trading course: <https://lumiwealth.com/algorithmic-trading-landing-page>**

**For our Machine Learning for Trading course: <https://www.lumiwealth.com/product-category/machine-learning-purchase/>**

**For our Options Trading course: <https://www.lumiwealth.com/product-category/options-trading-purchase/>**

## License

This library is covered by the MIT license for open sourced software which can be found here: <https://github.com/Lumiwealth/lumibot/blob/master/LICENSE>
