Backtesting and trading for stocks, options, crypto, futures and more!

# Documentation

**Check out the documentation for the project here: <http://lumibot.lumiwealth.com/>**

# Community

If you want to learn more about Lumibot or Algorithmic Trading then you will love out communities! You can join us on our forum or Discord.

**To join/view our forum: <https://lumiwealth.circle.so/c/lumibot/>**

**To join us on Discord: <https://discord.gg/TmMsJCKY3T>**

# Courses

If you need extra help building your algorithm, we have courses to help you out.

**For our Algorithmic Trading course: <https://lumiwealth.com/algorithmic-trading-landing-page>**

**For our Machine Learning for Trading course: <https://www.lumiwealth.com/product-category/machine-learning-purchase/>**

**For our Options Trading course: <https://www.lumiwealth.com/product-category/options-trading-purchase/>**

# License

This library is covered by the MIT license for open sourced software which can be found here: <https://github.com/Lumiwealth/lumibot/blob/master/LICENSE>

# Profiling and Optimization Tips to Improve Performance

## Profiling

We recommend using yappi to profile your code. You can install it with `pip install yappi`. You can then use it to profile your code like this:

```python
import yappi

yappi.start()
# Run your code here, eg. a backtest
MachineLearningLongShort.backtest(
    PandasDataBacktesting,
    backtesting_start,
    backtesting_end,
    pandas_data=pandas_data,
    benchmark_asset="TQQQ",
)
# Stop the profiler
yappi.stop()

# Save the results
threads = yappi.get_thread_stats()
for thread in threads:
    print(
        "Function stats for (%s) (%d)" % (thread.name, thread.id)
    )  # it is the Thread.__class__.__name__
    yappi.get_func_stats(ctx_id=thread.id).save(
        f"profile_{thread.name}.out", type="pstat"
    )

```

## Viewing the results

We recommend using snakeviz to view the results. You can install it with `pip install snakeviz`. You can then use it to view the results like this:

```bash
snakeviz profile_MachineLearningLongShort.out
```
