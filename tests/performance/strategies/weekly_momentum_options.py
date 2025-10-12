from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, TradingFee, Order
from lumibot.backtesting import ThetaDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import IS_BACKTESTING
import pandas as pd
from datetime import timedelta


''' 
This code was generated based on the user prompt: 'Say, make me a strategy that buys at-the-money or even slightly in-the-money call options for these tickers that I've listed here. Okay, but now some important things: we should buy the expiration. I think we buy it expiring like two or three weeks out. Something like that is right. We don't have to be exact, but like at least two weeks out expiration and then uh we hold it for a week. And then let's just do this around uh do this every week. So let's say on Thursdays when the market opens we could go ahead and buy um one of these tickers. The options are the actually let's say um depending on how much money we have. Right? So if we're going to be using uh actually we want to make this for any sized account, so we would want to take a look at the size of the and we don't want to be making like giant trades, right? We want to make something that you know, isn't taking up a huge amount of the account. We maybe want to make it that it's like five or ten percent of the account at a time. We don't want to be trading huge amounts. Um so I think that's probably just going to be one symbol at a time or maybe we could fit in two, right? I think it'd be good to diversify like that. I think depending on the account size we can do that but let's keep it to you know five to ten percent of the account. And if we can't get you know one symbol five to seven percent of the account then we could do the next symbol so we could do like a failover thing like that. Basically, what I want you to do is I want you to take a look at this list that I'm going to give you and I want you to go through this list of tickers and figure out which of these tickers is up the most over the past let's say three months. Pick the ticker that's up the most and we're going to trade options in that for the day, right? So uh again we're trading every Thursday. We want to hold it for a week. Maybe we want to even put in some take profits as well, so if we are up like a hundred percent or something we probably want to take that profit. And if we drop by you know quite a bit then maybe we want to take a stop loss as well, right? Maybe like a fifty percent stop loss or a hundred percent take profit or something like that. We'll want to hold it for a week. We'll want to do something that's like maybe two or three or four weeks out in terms of expiry dates. We want to buy at the money and we want to buy the tickers that are up the most over the past three months from this list of tickers I'm going to give you, okay? Make a strategy like that.

Large/Mega-cap or mid-large momentum (≈$10B+ mkt caps)
	•	HOOD
	•	VST
	•	STX
	•	WDC
	•	PLTR
	•	APP
	•	NRG
	•	CCJ

High-octane momentum (more volatile)
	•	CVNA
	•	HIMS'

'''


class WeeklyMomentumOptionsStrategy(Strategy):
    parameters = {
        # list of tickers provided by the user
        'symbols': ['HOOD', 'VST', 'STX', 'WDC', 'PLTR', 'APP', 'NRG', 'CCJ', 'CVNA', 'HIMS'],
        # allocation percent of account used per trade (0.05 = 5%, 0.10 = 10%)
        'allocation_pct': 0.07,
        # how many weeks to hold the option (1 week)
        'hold_weeks': 1,
        # minimum days until option expiration (14 days)
        'min_days_to_expiry': 14,
        # maximum days until option expiration (28 days)
        'max_days_to_expiry': 28,
        # take profit (100% gain)
        'take_profit_pct': 1.0,
        # stop loss (-50% loss)
        'stop_loss_pct': -0.5,
        # max number of different symbols to hold at once (1 or 2)
        'max_simultaneous': 1,
    }

    def initialize(self):
        # Called once when the strategy starts. We set up helpers and persistent variables.
        # Run once per day in backtests by default; set sleep timing if live.
        self.sleeptime = '1D'  # run once per day
        # Keep the market normal (US stocks). Options trade during market hours.
        # Options helper makes it easier to find expirations and strikes.
        self.options_helper = OptionsHelper(self)

        # Persistent storage for trades we made and entry info
        self.vars.trades = {}  # key: option symbol, value: dict with entry info
        self.vars.last_traded_week = None  # track weekly trades to avoid duplicate buys

        # Add a simple line so the chart shows the current cash level each day
        try:
            current_cash = self.get_cash()
            # add_line accepts numeric values; this helps debugging in the chart
            self.add_line('CASH', float(current_cash), color='blue')
        except Exception:
            # Logging but not failing if get_cash() isn't available at initialize for some brokers
            self.log_message('Could not add initial cash line', color='yellow')

    def on_trading_iteration(self):
        # Main lifecycle method that runs every iteration (daily for this script).
        dt = self.get_datetime()
        weekday = dt.weekday()  # Monday=0, Thursday=3

        # We only want to place new buys on Thursday market open.
        if weekday != 3:
            # Not Thursday, but we still need to manage exits for existing positions.
            self._manage_exits()
            return

        # If we already traded this week, skip buying to avoid multiple buys on Thursday.
        current_week = dt.isocalendar()[1]
        if self.vars.last_traded_week == current_week:
            self.log_message('Already traded this week, managing exits only.', color='yellow')
            self._manage_exits()
            return

        # It's Thursday and we haven't traded yet this week. Try to open new position(s).
        symbols = self.parameters.get('symbols', [])
        if not symbols:
            self.log_message('No symbols configured. Skipping.', color='red')
            return

        # Rank symbols by 3-month performance and pick top candidates
        ranked = self._rank_by_performance(symbols, lookback_days=63)
        if not ranked:
            self.log_message('Could not rank symbols (no data).', color='red')
            return

        allocation_pct = float(self.parameters.get('allocation_pct', 0.07))
        max_simultaneous = int(self.parameters.get('max_simultaneous', 1))
        bought_count = 0

        # Try candidates in rank order until we have filled up to max_simultaneous
        for symbol in ranked:
            if bought_count >= max_simultaneous:
                break

            # If we already have a position in this underlying, skip it
            pos = self.get_position(Asset(symbol, asset_type=Asset.AssetType.STOCK))
            if pos is not None and getattr(pos, 'quantity', 0) > 0:
                self.log_message(f'Already have a position in {symbol}, skipping.', color='yellow')
                continue

            # Try to build and submit an option buy for this symbol
            success = self._attempt_buy_for_symbol(symbol, allocation_pct)
            if success:
                bought_count += 1
                self.log_message(f'Bought option for {symbol}.', color='green')
            else:
                self.log_message(f'Failed to buy option for {symbol}, trying next.', color='yellow')

        # Mark we traded this week so we don't re-buy on the same Thursday
        self.vars.last_traded_week = current_week

        # After attempting buys, manage exits for any existing positions
        self._manage_exits()

    # Helper: rank symbols by 3-month performance
    def _rank_by_performance(self, symbols, lookback_days=63):
        perf = {}
        for s in symbols:
            try:
                asset = Asset(s, asset_type=Asset.AssetType.STOCK)
                bars = self.get_historical_prices(asset, lookback_days, 'day')
                if bars is None or bars.df.empty:
                    self.log_message(f'No historical data for {s}', color='red')
                    continue
                df = bars.df.dropna()
                if df.shape[0] < 2:
                    continue
                start = df['close'].iloc[0]
                end = df['close'].iloc[-1]
                pct = (end - start) / start
                perf[s] = pct
                self.log_message(
                    f'[RANK] {s}: start={start:.4f} end={end:.4f} pct_change={pct:.4f} (rows={df.shape[0]})',
                    color='blue'
                )
            except Exception as e:
                self.log_message(f'Error ranking {s}: {e}', color='red')
        # Return symbols sorted by performance descending
        ranked = sorted(perf.keys(), key=lambda x: perf[x], reverse=True)
        self.log_message(f'[RANK] ordered symbols: {ranked}', color='blue')
        return ranked

    # Helper: attempt to buy an option for a single symbol using allocation percent
    def _attempt_buy_for_symbol(self, symbol, allocation_pct):
        dt = self.get_datetime()
        underlying = Asset(symbol, asset_type=Asset.AssetType.STOCK)

        # Get option chains for the underlying
        chains_res = self.get_chains(underlying)
        if not chains_res:
            self.log_message(f'Option chains unavailable for {symbol}', color='red')
            return False

        # Choose an expiration at least min_days_to_expiry ahead, but not too far
        min_days = int(self.parameters.get('min_days_to_expiry', 14))
        max_days = int(self.parameters.get('max_days_to_expiry', 28))
        target_dt = dt + timedelta(days=min_days)

        expiry_date = self.options_helper.get_expiration_on_or_after_date(target_dt, chains_res, 'call')
        if not expiry_date:
            # If no expiration at that exact day, try a slightly later date up to max_days
            found = None
            for add_days in range(min_days, max_days + 1):
                try_dt = dt + timedelta(days=add_days)
                exp = self.options_helper.get_expiration_on_or_after_date(try_dt, chains_res, 'call')
                if exp:
                    found = exp
                    break
            expiry_date = found

        if not expiry_date:
            self.log_message(f'No suitable expiration found for {symbol}', color='red')
            return False

        # Find ATM or slightly ITM strike. We'll round the underlying price to nearest dollar.
        underlying_price = self.get_last_price(underlying)
        if underlying_price is None:
            self.log_message(f'No last price for {symbol}', color='red')
            return False
        self.log_message(
            f'[OPTION] underlying {symbol} last_price={float(underlying_price):.4f} target_expiry={expiry_date}',
            color='cyan'
        )

        rounded_price = round(float(underlying_price))
        # Prefer ATM, allow 1 strike ITM (lower strike for call) to be slightly ITM
        option_asset = self.options_helper.find_next_valid_option(underlying, rounded_price, expiry_date, put_or_call='call')
        if not option_asset:
            # Try slightly ITM (one dollar below rounded price)
            option_asset = self.options_helper.find_next_valid_option(underlying, rounded_price - 1, expiry_date, put_or_call='call')

        if not option_asset:
            self.log_message(f'No valid option strike found for {symbol} at expiry {expiry_date}', color='red')
            return False
        self.log_message(
            f'[OPTION] candidate {option_asset.symbol} strike={option_asset.strike} expiry={option_asset.expiration}',
            color='cyan'
        )

        # Get quote for the option to estimate price (use mid price when possible)
        quote = self.get_quote(option_asset)
        if quote is None or quote.mid_price is None:
            # Try last price fallback
            last = self.get_last_price(option_asset)
            if last is None:
                self.log_message(f'No quote for option {option_asset}', color='red')
                return False
            mid = float(last)
            self.log_message(
                f'[OPTION] {option_asset.symbol} quote missing mid; last_price used {mid:.4f}',
                color='yellow'
            )
        else:
            mid = float(quote.mid_price)
            self.log_message(
                f'[OPTION] {option_asset.symbol} bid={quote.bid} ask={quote.ask} mid={quote.mid_price}',
                color='cyan'
            )

        # Cost per contract in dollars is mid * 100 (option multiplier)
        cost_per_contract = mid * 100.0
        if cost_per_contract <= 0:
            self.log_message('Invalid option price, skipping.', color='red')
            return False
        self.log_message(
            f'[OPTION] {option_asset.symbol} cost_per_contract={cost_per_contract:.2f} cash={float(self.get_cash()):.2f}',
            color='cyan'
        )

        cash = float(self.get_cash())
        target_cash = cash * float(allocation_pct)
        # Compute number of contracts we can buy with allocation; must be at least 1
        contracts = int(target_cash // cost_per_contract)
        if contracts < 1:
            self.log_message(f'Not enough cash to buy 1 contract for {symbol} using {allocation_pct*100:.1f}% allocation.', color='yellow')
            return False

        # Create a limit order at mid price to reduce slippage
        order = self.create_order(option_asset, contracts, Order.OrderSide.BUY, limit_price=mid)
        # Add a tag so we can identify our orders later
        order.tag = 'weekly_momentum_options'

        # Submit the order. In backtesting this will fill according to the model; in live it will submit.
        submitted = self.submit_order(order)
        if not submitted:
            self.log_message(f'Order submission failed for {symbol}', color='red')
            return False

        # Store pending trade info; we'll capture fill details in on_filled_order
        self.vars.trades[option_asset] = {
            'asset': option_asset,
            'underlying': symbol,
            'contracts': contracts,
            'entry_price': None,  # filled in on_filled_order
            'entry_dt': None,
            'expiry': expiry_date,
        }
        self.log_message(
            f'Submitted buy order for {contracts} contracts of {option_asset.symbol} '
            f'{option_asset.right} {option_asset.strike} exp {option_asset.expiration} at {mid}',
            color='green'
        )
        return True

    # Lifecycle callback when an order is filled
    def on_filled_order(self, position, order, price, quantity, multiplier):
        # This method is called when an order fills; we record the entry price and timestamp.
        option_asset = getattr(order, 'asset', None)
        if option_asset is None:
            return

        if option_asset in self.vars.trades:
            # Price is per-option contract (mid/last). Store it and timestamp.
            self.vars.trades[option_asset]['entry_price'] = float(price)
            self.vars.trades[option_asset]['entry_dt'] = self.get_datetime()
            self.log_message(
                f'Filled {quantity} of {option_asset.symbol} {option_asset.right} '
                f'{option_asset.strike} exp {option_asset.expiration} at {price}',
                color='blue'
            )

    # Check existing option positions and exit if TP/SL or time-based exit
    def _manage_exits(self):
        # Iterate through our stored trades and current positions to decide exits
        to_remove = []
        for opt_asset, info in list(self.vars.trades.items()):
            # If we don't have an entry_price yet, skip until fill is recorded
            entry_price = info.get('entry_price')
            if entry_price is None:
                continue

            # Find the current price for the option
            option_asset = info.get('asset', opt_asset)
            try:
                quote = self.get_quote(option_asset)
                if quote is None or quote.mid_price is None:
                    current = self.get_last_price(option_asset)
                    if current is None:
                        self.log_message(
                            f'No current price for {option_asset.symbol} {option_asset.right} '
                            f'{option_asset.strike} exp {option_asset.expiration}',
                            color='yellow'
                        )
                        continue
                    current_mid = float(current)
                else:
                    current_mid = float(quote.mid_price)
            except Exception:
                # Some brokers return option symbol details differently; try to find position by symbol
                # Fallback: iterate positions and match by symbol
                pos = self.get_position(option_asset)
                if pos is None:
                    self.log_message(
                        f'Could not find position or price for '
                        f'{option_asset.symbol} {option_asset.right} {option_asset.strike} exp {option_asset.expiration}',
                        color='yellow'
                    )
                    continue
                current_mid = self.get_last_price(pos.asset)
                if current_mid is None:
                    continue

            # Compute percent change since entry (based on option premium)
            try:
                pnl_pct = (current_mid - entry_price) / entry_price
            except Exception:
                pnl_pct = 0

            # Time-based exit: if held for hold_weeks weeks or more
            entry_dt = info.get('entry_dt')
            held_days = None
            if entry_dt is not None:
                held_days = (self.get_datetime().date() - entry_dt.date()).days

            hold_limit_days = int(self.parameters.get('hold_weeks', 1)) * 7
            take_profit = float(self.parameters.get('take_profit_pct', 1.0))
            stop_loss = float(self.parameters.get('stop_loss_pct', -0.5))

            # If TP reached, SL reached, or time to exit, then sell all contracts
            exit_reason = None
            if pnl_pct >= take_profit:
                exit_reason = 'take_profit'
            elif pnl_pct <= stop_loss:
                exit_reason = 'stop_loss'
            elif held_days is not None and held_days >= hold_limit_days:
                exit_reason = 'time_exit'

            if exit_reason is not None:
                # Find the position object to get the exact quantity to sell
                # We search positions and match by option symbol
                positions = self.get_positions() or []
                found = None
                for p in positions:
                    try:
                        if p.asset == option_asset:
                            found = p
                            break
                    except Exception:
                        continue

                if found is None:
                    self.log_message(f'Position for {opt_symbol} not found at exit time', color='yellow')
                    # Remove trade record to avoid infinite loop
                    to_remove.append(opt_asset)
                    continue

                qty = found.quantity
                if qty is None or qty == 0:
                    to_remove.append(opt_asset)
                    continue

                # Create a sell order for all contracts at market to exit quickly
                sell_order = self.create_order(found.asset, qty, Order.OrderSide.SELL)
                sell_order.tag = 'weekly_momentum_options_exit'
                self.submit_order(sell_order)
                self.log_message(
                    f'Submitted exit for {option_asset.symbol} {option_asset.right} '
                    f'{option_asset.strike} exp {option_asset.expiration} due to {exit_reason}',
                    color='blue'
                )

                # Clean up stored trade record; we'll also handle fill confirmations in callbacks
                to_remove.append(opt_asset)

        # Remove trades we've scheduled for removal
        for s in to_remove:
            if s in self.vars.trades:
                del self.vars.trades[s]

    # Optional lifecycle hooks for logging and safety
    def on_canceled_order(self, order):
        self.log_message(f'Order canceled: {getattr(order, "asset", "unknown")}', color='yellow')

    def on_parameters_updated(self, parameters: dict):
        self.log_message('Parameters updated', color='blue')


if __name__ == '__main__':
    # The main block handles backtesting vs live trading automatically based on environment
    if IS_BACKTESTING:
        # Backtesting path: use PolygonDataBacktesting because we trade options
        trading_fee = TradingFee(percent_fee=0.001)

        params = {
            'symbols': ['HOOD', 'VST', 'STX', 'WDC', 'PLTR', 'APP', 'NRG', 'CCJ', 'CVNA', 'HIMS'],
            'allocation_pct': 0.07,
            'hold_weeks': 1,
            'min_days_to_expiry': 14,
            'max_days_to_expiry': 28,
            'take_profit_pct': 1.0,
            'stop_loss_pct': -0.5,
            'max_simultaneous': 1,
        }

        # Run the backtest. Start and end dates default to environment variables.
        result = WeeklyMomentumOptionsStrategy.backtest(
            ThetaDataBacktesting,
            benchmark_asset=Asset('SPY', Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset('USD', Asset.AssetType.FOREX),
            parameters=params,
            budget=100000,
        )

        # The backtest method will return results; print a short message in the console
        print('Backtest completed.')
    else:
        # Live trading path: instantiate the trader and add the strategy
        trader = Trader()
        strategy = WeeklyMomentumOptionsStrategy(
            quote_asset=Asset('USD', Asset.AssetType.FOREX),
            parameters={
                'symbols': ['HOOD', 'VST', 'STX', 'WDC', 'PLTR', 'APP', 'NRG', 'CCJ', 'CVNA', 'HIMS'],
                'allocation_pct': 0.07,
            }
        )
        trader.add_strategy(strategy)
        # Run all strategies that were added to the trader
        strategies = trader.run_all()
