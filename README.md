Backtesting and trading for stocks, options, crypto, futures and more!

# Documentation

To get started with Lumibot, you can check out our documentation below.

**Check out the documentation for the project here: <http://lumibot.lumiwealth.com/>**

# Running Tests

We use pytest for our testing framework. To run the tests, you can run the following command:

```bash
pytest
```

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

# Community

If you want to learn more about Lumibot or Algorithmic Trading then you will love out communities! You can join us on Discord.

**Join us on Discord: <https://discord.gg/TmMsJCKY3T>**

# Courses

If you need extra help building your algorithm, we have courses to help you out.

**For our Algorithmic Trading course: <https://lumiwealth.com/algorithmic-trading-landing-page>**

**For our Machine Learning for Trading course: <https://www.lumiwealth.com/product-category/machine-learning-purchase/>**

**For our Options Trading course: <https://www.lumiwealth.com/product-category/options-trading-purchase/>**

# License

This library is covered by the MIT license for open sourced software which can be found here: <https://github.com/Lumiwealth/lumibot/blob/master/LICENSE>
