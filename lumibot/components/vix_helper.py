from datetime import timedelta
from lumibot.entities import Asset
import pandas as pd
import yfinance as yf
from scipy import stats
import pandas_ta as ta
import traceback
import datetime

""" 
    Description
    -----------

    This is a general component for working with the VIX. It can be used to check the 
    VIX, VIX 1D, VIX RSI, VIX percentile values and more.
"""

class VixHelper:
    def __init__(self, strategy) -> None:
        """ 
            Initialize the VIX helper with the given strategy.

            Parameters
            ----------
            strategy : Strategy
                The strategy to use for the VIX helper.

            Returns
            -------
            None
        """
        self.strategy = strategy

        # Set the initial values for the variables
        self.last_historical_vix_update = None 
        self.last_historical_vix_1d_update = None
        self.last_historical_gvz_update = None

    def check_max_vix_1d(self, dt, max_vix_1d, use_open=False):
        """
        Check if the VIX 1D is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX 1D value.
        max_vix_1d : float
            The maximum VIX 1D value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX

        Returns
        -------
        bool
            True if the VIX 1D is too high, False otherwise.
        """
        # Get the VIX 1D value
        vix_1d = self.get_vix_1d_value(dt, use_open=use_open)

        # Plot the VIX 1D value
        self.strategy.add_line("vix_1d", vix_1d)

        # Check if the VIX 1D is greater than the maximum VIX 1D
        if max_vix_1d is not None and vix_1d > max_vix_1d:
            # Log message that the VIX 1D is too high
            self.strategy.log_message(
                f"VIX 1D is too high: {vix_1d} which is greater than the max of {max_vix_1d}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_1d_too_high", symbol="circle", color="blue", detail_text=f"VIX 1D too high: {vix_1d}"
            )

            return True
        
        # Log message that the VIX 1D is within the limits
        self.strategy.log_message(
            f"VIX 1D is within the limits: {vix_1d} which is less than the max of {max_vix_1d}",
            color="green", broadcast=True
        )

        return False
    
    def check_min_vix_1d(self, dt, min_vix_1d, use_open=False):
        """
        Check if the VIX 1D is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX 1D value.
        min_vix_1d : float
            The minimum VIX 1D value to create a condor.

        Returns
        -------
        bool
            True if the VIX 1D is too low, False otherwise.
        """
        # Get the VIX 1D value
        vix_1d = self.get_vix_1d_value(dt, use_open=use_open)

        # Check if the VIX 1D is greater than the minimum VIX 1D
        if min_vix_1d is not None and vix_1d < min_vix_1d:
            # Log message that the VIX 1D is too low
            self.strategy.log_message(
                f"VIX 1D is too low: {vix_1d} which is less than the min of {min_vix_1d}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_1d_too_low", symbol="circle", color="blue", detail_text=f"VIX 1D too low: {vix_1d}"
            )

            return True
        
        return False
    
    def check_max_vix_percentile(self, dt, max_vix_percentile, vix_percentile_window, use_open=False):
        """
        Check if the VIX is too high based on the percentile. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        max_vix_percentile : float
            The maximum VIX percentile value to create a condor.
        vix_percentile_window : int
            The window (in days) to calculate the VIX percentile.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too high based on the percentile, False otherwise.
        """
        # Check if the max VIX percentile is not None
        if max_vix_percentile is None:
            return False
        
        # Check if the VIX percentile window is not None
        if vix_percentile_window is None:
            return False

        # Get the VIX percentile value
        vix_percentile = self.get_vix_percentile(dt, vix_percentile_window, use_open=use_open)

        # Check if the VIX percentile is not None
        if vix_percentile is None:
            return False

        # Check if the VIX is greater than the maximum VIX percentile
        if max_vix_percentile is not None and vix_percentile > max_vix_percentile:
            # Log message that the VIX is too high based on the percentile
            self.strategy.log_message(
                f"VIX is too high based on the percentile: {vix_percentile} which is greater than the max of {max_vix_percentile}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_percentile_too_high", symbol="circle", color="blue", detail_text=f"VIX too high based on the percentile: {vix_percentile}"
            )

            return True
        
        # Log message that the VIX is within the limits based on the percentile
        self.strategy.log_message(
            f"VIX is within the limits based on the percentile: {vix_percentile} which is less than the max of {max_vix_percentile}",
            color="green", broadcast=True
        )
        
        return False
    
    def get_vix_percentile(self, dt, window, use_open=False):
        """
        Get the VIX percentile value for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the VIX percentile value.
        window : int
            The window (in days) to calculate the VIX percentile.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX

        Returns
        -------
        float
            The VIX percentile value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Check if window is not None
        if window is None:
            return None

        # Get the VIX values for the window
        vix_values = self.get_vix_values(dt, window, use_open=use_open)

        # Check if vix_values is not None
        if vix_values is None or len(vix_values) == 0:
            return None

        # Get the current VIX value
        vix = self.get_vix_value(dt, use_open=use_open)
        if vix is None:
            return None

        # Get the VIX percentile value
        vix_percentile = stats.percentileofscore(vix_values, vix)

        # Add a marker to the chart
        self.strategy.add_marker(
            "vix_percentile", symbol="square", color="blue", value=vix_percentile, detail_text=f"VIX percentile: {vix_percentile}"
        )

        return vix_percentile

    def check_max_vix(self, dt, max_vix, use_open=False):
        """
        Check if the VIX is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        max_vix : float
            The maximum VIX value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too high, False otherwise.
        """
        # Get the VIX value
        vix = self.get_vix_value(dt, use_open=use_open)

        # Plot the VIX value
        if vix is not None:
            self.strategy.add_line("vix", vix)
            self.strategy.add_marker("vix", symbol="square", color="blue", detail_text=f"VIX: {vix}", value=vix)

        # Check if the VIX is greater than the maximum VIX
        if vix is None or max_vix is None:
            return False
        if vix > max_vix:
            # Log message that the VIX is too high
            self.strategy.log_message(
                f"VIX is too high: {vix} which is greater than the max of {max_vix}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_too_high", symbol="circle", color="blue", detail_text=f"VIX too high: {vix}"
            )

            return True
        
        # Log message that the VIX is within the limits
        self.strategy.log_message(
            f"VIX is within the limits: {vix} which is less than the max of {max_vix}",
            color="green", broadcast=True
        )
        
        return False
    
    def check_min_vix(self, dt, min_vix, use_open=False):
        """
        Check if the VIX is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        min_vix : float
            The minimum VIX value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too low, False otherwise.
        """
        # Get the VIX value
        vix = self.get_vix_value(dt, use_open=use_open)

        if vix is None or min_vix is None:
            return False

        if vix < min_vix:
            # Log message that the VIX is too low
            self.strategy.log_message(
                f"VIX is too low: {vix} which is less than the min of {min_vix}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_too_low", symbol="circle", color="blue", detail_text=f"VIX too low: {vix}"
            )

            return True
        
        return False
    
    def check_max_vix_rsi(self, dt, max_vix_rsi, rsi_window=14, use_open=False):
        """
        Check if the VIX RSI is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX RSI value.
        max_vix_rsi : float
            The maximum VIX RSI value to create a condor.
        rsi_window : int
            The window (in days) to calculate the VIX RSI.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX RSI value.

        Returns
        -------
        bool
            True if the VIX RSI is too high, False otherwise.
        """
        # If max VIX RSI is None, then return False
        if max_vix_rsi is None:
            return False

        # Get the VIX RSI value
        vix_rsi = self.get_vix_rsi_value(dt, rsi_window, use_open=use_open)

        if vix_rsi is None:
            return False

        # Plot the VIX RSI value
        self.strategy.add_line("vix_rsi", vix_rsi)

        # Check if the VIX RSI is greater than the maximum VIX RSI
        if max_vix_rsi is not None and vix_rsi > max_vix_rsi:
            # Log message that the VIX RSI is too high
            self.strategy.log_message(
                f"VIX RSI is too high: {vix_rsi} which is greater than the max of {max_vix_rsi}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_rsi_too_high", symbol="circle", color="blue", detail_text=f"VIX RSI too high: {vix_rsi}"
            )

            return True
        
        # Log message that the VIX RSI is within the limits
        self.strategy.log_message(
            f"VIX RSI is within the limits: {vix_rsi} which is less than the max of {max_vix_rsi}",
            color="green", broadcast=True
        )
        
        return False
    
    def get_vix_rsi_value(self, dt, window=14, use_open=False):
        """
        Get the VIX RSI value for the given datetime.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the VIX RSI value.
        window : int
            The window (in days) to calculate the VIX RSI.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX RSI value.

        Returns
        -------
        float
            The VIX RSI value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Data window. Get a 60% larger window for the VIX RSI calculation because of weekends and holidays
        download_window = int(window * 1.6)

        # Get the VIX values for the window
        vix_values = self.get_vix_values(dt, download_window, use_open=use_open)

        # Check if vix_values is not None
        if vix_values is None or len(vix_values) == 0:
            return None
        
        # Convert the list to a pandas DataFrame
        vix_df = pd.DataFrame(vix_values, columns=['VIX'])

        # Calculate the RSI
        vix_df['RSI'] = ta.rsi(vix_df['VIX'], length=window)

        # Get the last VIX RSI value
        vix_rsi = vix_df['RSI'].iloc[-1]

        return vix_rsi
    
    def get_vix_values(self, dt, window, use_open=False):
        """
        Get the VIX values for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the VIX values.
        window : int
            The window (in days) to calculate the VIX values.
        use_open : bool
            Whether to use the open price of the VIX.

        Returns
        -------
        list
            The VIX values for the window.
        """
        # Check if the window is None
        if window is None:
            return None

        # Check if the current date is None
        if dt is None:
            return None
        
        # If the current date is None, then return a super high value so it doesn't trigger trades
        if dt is None:
            return None

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date to avoid lookahead bias
            previous_dt = dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # --- FIX: Normalize tz-awareness for index and lookup datetimes ---
            if hasattr(self, 'historical_vix'):
                idx = self.historical_vix.index
                if idx.tz is not None and today_date.tzinfo is None:
                    today_date = today_date.tz_localize(idx.tz)
                    previous_date = previous_date.tz_localize(idx.tz)
                elif idx.tz is None and today_date.tzinfo is not None:
                    today_date = today_date.tz_localize(None)
                    previous_date = previous_date.tz_localize(None)
            # --- END FIX ---

            # Check if the historical VIX data is up to date
            if self.last_historical_vix_update is not None and self.last_historical_vix_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_vix.index.asof(today_date)
                    vix_values = self.historical_vix.loc[nearest_date - timedelta(days=window):nearest_date]['Open'].tolist()
                    return vix_values
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_vix.index.asof(previous_date)
                    vix_values = self.historical_vix.loc[nearest_date - timedelta(days=window):nearest_date]['Close'].tolist()
                    return vix_values
            
            vix = yf.Ticker("^VIX")
            self.historical_vix = vix.history(period="max")

            # Set the Date column as the index
            self.historical_vix.index = pd.to_datetime(self.historical_vix.index)

            # Set the last historical VIX date as the current date
            self.last_historical_vix_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_vix.index.asof(today_date)
                vix_values = self.historical_vix.loc[nearest_date - timedelta(days=window):nearest_date]['Open'].tolist()
                return vix_values
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_vix.index.asof(previous_date)
                vix_values = self.historical_vix.loc[nearest_date - timedelta(days=window):nearest_date]['Close'].tolist()
                return vix_values

        except Exception as e:
            self.strategy.log_message(f"ERROR: Failed to fetch live VIX values: {e}", color="red", broadcast=True)
            return None
    
    def get_vix_value(self, current_dt=None, use_open=False):
        """
        Get the VIX value for the current date.

        Parameters
        ----------
        current_dt : datetime.datetime
            The current datetime to get the VIX value.
        use_open : bool
            Whether to use the open price of the VIX.

        Returns
        -------
        float
            The VIX value for the current date.
        """

        # If the current date is None, then return a super high value so it doesn't trigger trades
        if current_dt is None:
            return None

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date
            previous_dt = current_dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(current_dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # Check if the historical VIX data is up to date
            if self.last_historical_vix_update is not None and self.last_historical_vix_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_vix.index.asof(today_date)
                    vix_val = self.historical_vix.loc[nearest_date]['Open']
                    return vix_val
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_vix.index.asof(previous_date)
                    vix_val = self.historical_vix.loc[nearest_date]['Close']
                    return vix_val
            
            vix = yf.Ticker("^VIX")
            self.historical_vix = vix.history(period="max")

            # Set the Date column as the index
            self.historical_vix.index = pd.to_datetime(self.historical_vix.index)

            # Set the last historical VIX date as the current date
            self.last_historical_vix_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_vix.index.asof(today_date)
                vix_val = self.historical_vix.loc[nearest_date]['Open']
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_vix.index.asof(previous_date)
                vix_val = self.historical_vix.loc[nearest_date]['Close']

            # Add a marker to the chart to show the VIX value
            self.strategy.add_marker(
                "vix_value",
                value=vix_val,
                symbol="square",
                color="blue",
            )

            return vix_val
        
        except Exception as e:
            self.strategy.log_message(f"ERROR: Failed to fetch live VIX value: {e}", color="red", broadcast=True)
            return None  # Return None if unable to fetch
    
    def get_vix_1d_value(self, current_dt=None, use_open=False):
        """
        Get the VIX 1D value for the current date.

        Parameters
        ----------
        current_dt : datetime.datetime
            The current datetime to get the VIX 1D value.
        use_open : bool
            Whether to use the open price of the VIX 1D.

        Returns
        -------
        float
            The VIX 1D value for the current date.
        """

        # If the current date is None, then return a super high value so it doesn't trigger trades
        if current_dt is None:
            return 1000

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date
            previous_dt = current_dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(current_dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # --- FIX: Normalize tz-awareness for index and lookup datetimes ---
            if hasattr(self, 'historical_vix_1d'):
                idx = self.historical_vix_1d.index
                if idx.tz is not None and today_date.tzinfo is None:
                    today_date = today_date.tz_localize(idx.tz)
                    previous_date = previous_date.tz_localize(idx.tz)
                elif idx.tz is None and today_date.tzinfo is not None:
                    today_date = today_date.tz_localize(None)
                    previous_date = previous_date.tz_localize(None)
            # --- END FIX ---

            # Check if the historical VIX 1D data is up to date
            if self.last_historical_vix_1d_update is not None and self.last_historical_vix_1d_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_vix_1d.index.asof(today_date)
                    vix_val = self.historical_vix_1d.loc[nearest_date]['Open']
                    return vix_val
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_vix_1d.index.asof(previous_date)
                    vix_val = self.historical_vix_1d.loc[nearest_date]['Close']
                    return vix_val
            
            vix_1d = yf.Ticker("^VIX1D")
            self.historical_vix_1d = vix_1d.history(period="max")

            # Set the Date column as the index
            self.historical_vix_1d.index = pd.to_datetime(self.historical_vix_1d.index)

            # Set the last historical VIX 1D date as the current date
            self.last_historical_vix_1d_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_vix_1d.index.asof(today_date)
                vix_val = self.historical_vix_1d.loc[nearest_date]['Open']
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_vix_1d.index.asof(previous_date)
                vix_val = self.historical_vix_1d.loc[nearest_date]['Close']

            # Add a marker to the chart to show the VIX 1D value
            self.strategy.add_marker(
                "vix_1d_value",
                value=vix_val,
                symbol="square",
                color="blue",
            )

            return vix_val

        except Exception as e:
            # Get the traceback
            self.strategy.log_message(f"ERROR: Failed to fetch live VIX 1D value: {e}", color="red", broadcast=True)
            return 1000

    def get_gvz_value(self, current_dt=None, use_open=False):
        """
        Get the GVZ value for the current date.

        Parameters
        ----------
        current_dt : datetime.datetime
            The current datetime to get the GVZ value.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        float
            The GVZ value for the current date.
        """

        # If the current date is None, then return a super high value so it doesn't trigger trades
        if current_dt is None:
            return 1000

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date
            previous_dt = current_dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(current_dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # --- FIX: Normalize tz-awareness for index and lookup datetimes ---
            if hasattr(self, 'historical_gvz'):
                idx = self.historical_gvz.index
                if idx.tz is not None and today_date.tzinfo is None:
                    today_date = today_date.tz_localize(idx.tz)
                    previous_date = previous_date.tz_localize(idx.tz)
                elif idx.tz is None and today_date.tzinfo is not None:
                    today_date = today_date.tz_localize(None)
                    previous_date = previous_date.tz_localize(None)
            # --- END FIX ---

            # Check if the historical GVZ data is up to date
            if self.last_historical_gvz_update is not None and self.last_historical_gvz_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_gvz.index.asof(today_date)
                    gvz_val = self.historical_gvz.loc[nearest_date]['Open']
                    return gvz_val
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_gvz.index.asof(previous_date)
                    gvz_val = self.historical_gvz.loc[nearest_date]['Close']
                    return gvz_val
            
            gvz = yf.Ticker("^GVZ")
            self.historical_gvz = gvz.history(period="max")

            # Set the Date column as the index
            self.historical_gvz.index = pd.to_datetime(self.historical_gvz.index)

            # Set the last historical GVZ date as the current date
            self.last_historical_gvz_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_gvz.index.asof(today_date)
                gvz_val = self.historical_gvz.loc[nearest_date]['Open']
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_gvz.index.asof(previous_date)
                gvz_val = self.historical_gvz.loc[nearest_date]['Close']

            # Add a marker to the chart to show the GVZ value
            self.strategy.add_marker(
                "gvz_value",
                value=gvz_val,
                symbol="square",
                color="blue",
            )

            return gvz_val

        except Exception as e:
            # Get the traceback
            self.strategy.log_message(f"ERROR: Failed to fetch live GVZ value: {e}", color="red", broadcast=True)
            traceback.print_exc()
            return 1000

    def check_max_gvz(self, dt, max_gvz, use_open=False):
        """
        Check if the GVZ is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the GVZ value.
        max_gvz : float
            The maximum GVZ value to create a condor.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        bool
            True if the GVZ is too high, False otherwise.
        """
        # Get the GVZ value
        gvz = self.get_gvz_value(dt, use_open=use_open)

        # Plot the GVZ value
        if gvz is not None:
            self.strategy.add_line("gvz", gvz)
            self.strategy.add_marker("gvz", symbol="square", color="blue", detail_text=f"GVZ: {gvz}", value=gvz)

        # Check if the GVZ is greater than the maximum GVZ
        if gvz is None or max_gvz is None:
            return False
        if gvz > max_gvz:
            # Log message that the GVZ is too high
            self.strategy.log_message(
                f"GVZ is too high: {gvz} which is greater than the max of {max_gvz}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "gvz_too_high", symbol="circle", color="blue", detail_text=f"GVZ too high: {gvz}"
            )

            return True
        
        # Log message that the GVZ is within the limits
        self.strategy.log_message(
            f"GVZ is within the limits: {gvz} which is less than the max of {max_gvz}",
            color="green", broadcast=True
        )
        
        return False

    def check_min_gvz(self, dt, min_gvz, use_open=False):
        """
        Check if the GVZ is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the GVZ value.
        min_gvz : float
            The minimum GVZ value to create a condor.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        bool
            True if the GVZ is too low, False otherwise.
        """
        # Get the GVZ value
        gvz = self.get_gvz_value(dt, use_open=use_open)

        if gvz is None or min_gvz is None:
            return False
        if gvz < min_gvz:
            # Log message that the GVZ is too low
            self.strategy.log_message(
                f"GVZ is too low: {gvz} which is less than the min of {min_gvz}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "gvz_too_low", symbol="circle", color="blue", detail_text=f"GVZ too low: {gvz}"
            )

            return True
        
        return False

    def check_max_gvz_percentile(self, dt, max_gvz_percentile, gvz_percentile_window, use_open=False):
        """
        Check if the GVZ is too high based on the percentile. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the GVZ value.
        max_gvz_percentile : float
            The maximum GVZ percentile value to create a condor.
        gvz_percentile_window : int
            The window (in days) to calculate the GVZ percentile.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        bool
            True if the GVZ is too high based on the percentile, False otherwise.
        """
        # Check if the max GVZ percentile is not None
        if max_gvz_percentile is None:
            return False
        
        # Check if the GVZ percentile window is not None
        if gvz_percentile_window is None:
            return False

        # Get the GVZ percentile value
        gvz_percentile = self.get_gvz_percentile(dt, gvz_percentile_window, use_open=use_open)

        # Check if the GVZ percentile is not None
        if gvz_percentile is None:
            return False

        # Check if the GVZ is greater than the maximum GVZ percentile
        if max_gvz_percentile is not None and gvz_percentile > max_gvz_percentile:
            # Log message that the GVZ is too high based on the percentile
            self.strategy.log_message(
                f"GVZ is too high based on the percentile: {gvz_percentile} which is greater than the max of {max_gvz_percentile}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "gvz_percentile_too_high", symbol="circle", color="blue", detail_text=f"GVZ too high based on the percentile: {gvz_percentile}"
            )

            return True
        
        return False

    def get_gvz_percentile(self, dt, window, use_open=False):
        """
        Get the GVZ percentile value for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the GVZ percentile value.
        window : int
            The window (in days) to calculate the GVZ percentile.
        use_open : bool
            Whether to use the open price of the GVZ

        Returns
        -------
        float
            The GVZ percentile value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Check if window is not None
        if window is None:
            return None

        # Get the GVZ values for the window
        gvz_values = self.get_gvz_values(dt, window, use_open=use_open)

        if gvz_values is None or len(gvz_values) == 0:
            return None

        gvz = self.get_gvz_value(dt, use_open=use_open)
        if gvz is None:
            return None

        # Get the GVZ percentile value
        gvz_percentile = stats.percentileofscore(gvz_values, gvz)

        # Add a marker to the chart
        self.strategy.add_marker(
            "gvz_percentile", symbol="square", color="blue", value=gvz_percentile, detail_text=f"GVZ percentile: {gvz_percentile}"
        )

        return gvz_percentile

    def get_gvz_values(self, dt, window, use_open=False):
        """
        Get the GVZ values for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the GVZ values.
        window : int
            The window (in days) to calculate the GVZ values.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        list
            The GVZ values for the window.
        """
        # Check if the window is None
        if window is None:
            return None

        # Check if the current date is None
        if dt is None:
            return None
        
        if dt is None:
            return None

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date to avoid lookahead bias
            previous_dt = dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # --- FIX: Normalize tz-awareness for index and lookup datetimes ---
            if hasattr(self, 'historical_gvz'):
                idx = self.historical_gvz.index
                if idx.tz is not None and today_date.tzinfo is None:
                    today_date = today_date.tz_localize(idx.tz)
                    previous_date = previous_date.tz_localize(idx.tz)
                elif idx.tz is None and today_date.tzinfo is not None:
                    today_date = today_date.tz_localize(None)
                    previous_date = previous_date.tz_localize(None)
            # --- END FIX ---

            # Check if the historical GVZ data is up to date
            if self.last_historical_gvz_update is not None and self.last_historical_gvz_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_gvz.index.asof(today_date)
                    gvz_values = self.historical_gvz.loc[nearest_date - timedelta(days=window):nearest_date]['Open'].tolist()
                    return gvz_values
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_gvz.index.asof(previous_date)
                    gvz_values = self.historical_gvz.loc[nearest_date - timedelta(days=window):nearest_date]['Close'].tolist()
                    return gvz_values
            
            gvz = yf.Ticker("^GVZ")
            self.historical_gvz = gvz.history(period="max")

            # Set the Date column as the index
            self.historical_gvz.index = pd.to_datetime(self.historical_gvz.index)

            # Set the last historical GVZ date as the current date
            self.last_historical_gvz_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_gvz.index.asof(today_date)
                gvz_values = self.historical_gvz.loc[nearest_date - timedelta(days=window):nearest_date]['Open'].tolist()
                return gvz_values
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_gvz.index.asof(previous_date)
                gvz_values = self.historical_gvz.loc[nearest_date - timedelta(days=window):nearest_date]['Close'].tolist()
                return gvz_values

        except Exception as e:
            self.strategy.log_message(f"ERROR: Failed to fetch live GVZ values: {e}", color="red", broadcast=True)
            return None

    def get_gvz_rsi_value(self, dt, window=14, use_open=False):
        """
        Get the GVZ RSI value for the given datetime.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the GVZ RSI value.
        window : int
            The window (in days) to calculate the GVZ RSI.
        use_open : bool
            Whether to use the open price of the GVZ RSI value.

        Returns
        -------
        float
            The GVZ RSI value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Data window. Get a 60% larger window for the GVZ RSI calculation because of weekends and holidays
        download_window = int(window * 1.6)

        # Get the GVZ values for the window
        gvz_values = self.get_gvz_values(dt, download_window, use_open=use_open)

        if gvz_values is None or len(gvz_values) == 0:
            return None
        
        # Convert the list to a pandas DataFrame
        gvz_df = pd.DataFrame(gvz_values, columns=['GVZ'])

        # Calculate the RSI
        gvz_df['RSI'] = ta.rsi(gvz_df['GVZ'], length=window)

        # Get the last GVZ RSI value
        gvz_rsi = gvz_df['RSI'].iloc[-1]

        return gvz_rsi
    
    def check_max_vix_1d(self, dt, max_vix_1d, use_open=False):
        """
        Check if the VIX 1D is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX 1D value.
        max_vix_1d : float
            The maximum VIX 1D value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX

        Returns
        -------
        bool
            True if the VIX 1D is too high, False otherwise.
        """
        # Get the VIX 1D value
        vix_1d = self.get_vix_1d_value(dt, use_open=use_open)

        # Plot the VIX 1D value
        self.strategy.add_line("vix_1d", vix_1d)

        # Check if the VIX 1D is greater than the maximum VIX 1D
        if max_vix_1d is not None and vix_1d > max_vix_1d:
            # Log message that the VIX 1D is too high
            self.strategy.log_message(
                f"VIX 1D is too high: {vix_1d} which is greater than the max of {max_vix_1d}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_1d_too_high", symbol="circle", color="blue", detail_text=f"VIX 1D too high: {vix_1d}"
            )

            return True
        
        # Log message that the VIX 1D is within the limits
        self.strategy.log_message(
            f"VIX 1D is within the limits: {vix_1d} which is less than the max of {max_vix_1d}",
            color="green", broadcast=True
        )

        return False
    
    def check_min_vix_1d(self, dt, min_vix_1d, use_open=False):
        """
        Check if the VIX 1D is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX 1D value.
        min_vix_1d : float
            The minimum VIX 1D value to create a condor.

        Returns
        -------
        bool
            True if the VIX 1D is too low, False otherwise.
        """
        # Get the VIX 1D value
        vix_1d = self.get_vix_1d_value(dt, use_open=use_open)

        # Check if the VIX 1D is greater than the minimum VIX 1D
        if min_vix_1d is not None and vix_1d < min_vix_1d:
            # Log message that the VIX 1D is too low
            self.strategy.log_message(
                f"VIX 1D is too low: {vix_1d} which is less than the min of {min_vix_1d}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_1d_too_low", symbol="circle", color="blue", detail_text=f"VIX 1D too low: {vix_1d}"
            )

            return True
        
        return False
    
    def check_max_vix_percentile(self, dt, max_vix_percentile, vix_percentile_window, use_open=False):
        """
        Check if the VIX is too high based on the percentile. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        max_vix_percentile : float
            The maximum VIX percentile value to create a condor.
        vix_percentile_window : int
            The window (in days) to calculate the VIX percentile.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too high based on the percentile, False otherwise.
        """
        # Check if the max VIX percentile is not None
        if max_vix_percentile is None:
            return False
        
        # Check if the VIX percentile window is not None
        if vix_percentile_window is None:
            return False

        # Get the VIX percentile value
        vix_percentile = self.get_vix_percentile(dt, vix_percentile_window, use_open=use_open)

        # Check if the VIX percentile is not None
        if vix_percentile is None:
            return False

        # Check if the VIX is greater than the maximum VIX percentile
        if max_vix_percentile is not None and vix_percentile > max_vix_percentile:
            # Log message that the VIX is too high based on the percentile
            self.strategy.log_message(
                f"VIX is too high based on the percentile: {vix_percentile} which is greater than the max of {max_vix_percentile}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_percentile_too_high", symbol="circle", color="blue", detail_text=f"VIX too high based on the percentile: {vix_percentile}"
            )

            return True
        
        # Log message that the VIX is within the limits based on the percentile
        self.strategy.log_message(
            f"VIX is within the limits based on the percentile: {vix_percentile} which is less than the max of {max_vix_percentile}",
            color="green", broadcast=True
        )
        
        return False
    
    def get_vix_percentile(self, dt, window, use_open=False):
        """
        Get the VIX percentile value for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the VIX percentile value.
        window : int
            The window (in days) to calculate the VIX percentile.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX

        Returns
        -------
        float
            The VIX percentile value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Check if window is not None
        if window is None:
            return None

        # Get the VIX values for the window
        vix_values = self.get_vix_values(dt, window, use_open=use_open)

        # Check if vix_values is not None
        if vix_values is None or len(vix_values) == 0:
            return None

        # Get the current VIX value
        vix = self.get_vix_value(dt, use_open=use_open)
        if vix is None:
            return None

        # Get the VIX percentile value
        vix_percentile = stats.percentileofscore(vix_values, vix)

        # Add a marker to the chart
        self.strategy.add_marker(
            "vix_percentile", symbol="square", color="blue", value=vix_percentile, detail_text=f"VIX percentile: {vix_percentile}"
        )

        return vix_percentile

    def check_max_vix(self, dt, max_vix, use_open=False):
        """
        Check if the VIX is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        max_vix : float
            The maximum VIX value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too high, False otherwise.
        """
        # Get the VIX value
        vix = self.get_vix_value(dt, use_open=use_open)

        # Plot the VIX value
        if vix is not None:
            self.strategy.add_line("vix", vix)
            self.strategy.add_marker("vix", symbol="square", color="blue", detail_text=f"VIX: {vix}", value=vix)

        # Check if the VIX is greater than the maximum VIX
        if vix is None or max_vix is None:
            return False
        if vix > max_vix:
            # Log message that the VIX is too high
            self.strategy.log_message(
                f"VIX is too high: {vix} which is greater than the max of {max_vix}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_too_high", symbol="circle", color="blue", detail_text=f"VIX too high: {vix}"
            )

            return True
        
        # Log message that the VIX is within the limits
        self.strategy.log_message(
            f"VIX is within the limits: {vix} which is less than the max of {max_vix}",
            color="green", broadcast=True
        )
        
        return False
    
    def check_min_vix(self, dt, min_vix, use_open=False):
        """
        Check if the VIX is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX value.
        min_vix : float
            The minimum VIX value to create a condor.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX value.

        Returns
        -------
        bool
            True if the VIX is too low, False otherwise.
        """
        # Get the VIX value
        vix = self.get_vix_value(dt, use_open=use_open)

        if vix is None or min_vix is None:
            return False

        if vix < min_vix:
            # Log message that the VIX is too low
            self.strategy.log_message(
                f"VIX is too low: {vix} which is less than the min of {min_vix}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_too_low", symbol="circle", color="blue", detail_text=f"VIX too low: {vix}"
            )

            return True
        
        return False
    
    def check_max_vix_rsi(self, dt, max_vix_rsi, rsi_window=14, use_open=False):
        """
        Check if the VIX RSI is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the VIX RSI value.
        max_vix_rsi : float
            The maximum VIX RSI value to create a condor.
        rsi_window : int
            The window (in days) to calculate the VIX RSI.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX RSI value.

        Returns
        -------
        bool
            True if the VIX RSI is too high, False otherwise.
        """
        # If max VIX RSI is None, then return False
        if max_vix_rsi is None:
            return False

        # Get the VIX RSI value
        vix_rsi = self.get_vix_rsi_value(dt, rsi_window, use_open=use_open)

        if vix_rsi is None:
            return False

        # Plot the VIX RSI value
        self.strategy.add_line("vix_rsi", vix_rsi)

        # Check if the VIX RSI is greater than the maximum VIX RSI
        if max_vix_rsi is not None and vix_rsi > max_vix_rsi:
            # Log message that the VIX RSI is too high
            self.strategy.log_message(
                f"VIX RSI is too high: {vix_rsi} which is greater than the max of {max_vix_rsi}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "vix_rsi_too_high", symbol="circle", color="blue", detail_text=f"VIX RSI too high: {vix_rsi}"
            )

            return True
        
        # Log message that the VIX RSI is within the limits
        self.strategy.log_message(
            f"VIX RSI is within the limits: {vix_rsi} which is less than the max of {max_vix_rsi}",
            color="green", broadcast=True
        )
        
        return False
    
    def get_vix_rsi_value(self, dt, window=14, use_open=False):
        """
        Get the VIX RSI value for the given datetime.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the VIX RSI value.
        window : int
            The window (in days) to calculate the VIX RSI.
        use_open : bool
            Whether to use the open price of the underlying asset to get the VIX RSI value.

        Returns
        -------
        float
            The VIX RSI value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Data window. Get a 60% larger window for the VIX RSI calculation because of weekends and holidays
        download_window = int(window * 1.6)

        # Get the VIX values for the window
        vix_values = self.get_vix_values(dt, download_window, use_open=use_open)

        # Check if vix_values is not None
        if vix_values is None or len(vix_values) == 0:
            return None
        
        # Convert the list to a pandas DataFrame
        vix_df = pd.DataFrame(vix_values, columns=['VIX'])

        # Calculate the RSI
        vix_df['RSI'] = ta.rsi(vix_df['VIX'], length=window)

        # Get the last VIX RSI value
        vix_rsi = vix_df['RSI'].iloc[-1]

        return vix_rsi
    
    def get_gvz_value(self, current_dt=None, use_open=False):
        """
        Get the GVZ value for the current date.

        Parameters
        ----------
        current_dt : datetime.datetime
            The current datetime to get the GVZ value.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        float
            The GVZ value for the current date.
        """

        # If the current date is None, then return a super high value so it doesn't trigger trades
        if current_dt is None:
            return 1000

        try:
            # Get the actual dt for the current date to speed this up for backtesting
            actual_dt = datetime.datetime.now()

            # Subtract one day to get the previous day's date
            previous_dt = current_dt - timedelta(days=1)

            # Get today as a Timestamp
            today_date = pd.Timestamp(current_dt)

            # Get previous date as a Timestamp
            previous_date = pd.Timestamp(previous_dt)

            # --- FIX: Normalize tz-awareness for index and lookup datetimes ---
            if hasattr(self, 'historical_gvz'):
                idx = self.historical_gvz.index
                if idx.tz is not None and today_date.tzinfo is None:
                    today_date = today_date.tz_localize(idx.tz)
                    previous_date = previous_date.tz_localize(idx.tz)
                elif idx.tz is None and today_date.tzinfo is not None:
                    today_date = today_date.tz_localize(None)
                    previous_date = previous_date.tz_localize(None)
            # --- END FIX ---

            # Check if the historical GVZ data is up to date
            if self.last_historical_gvz_update is not None and self.last_historical_gvz_update.date() == actual_dt.date():
                # If we are using the open price, then return the open price for today
                if use_open:
                    # Get the row closest to today
                    nearest_date = self.historical_gvz.index.asof(today_date)
                    gvz_val = self.historical_gvz.loc[nearest_date]['Open']
                    return gvz_val
                else:
                    # Get the row closest to the previous date
                    nearest_date = self.historical_gvz.index.asof(previous_date)
                    gvz_val = self.historical_gvz.loc[nearest_date]['Close']
                    return gvz_val
            
            gvz = yf.Ticker("^GVZ")
            self.historical_gvz = gvz.history(period="max")

            # Set the Date column as the index
            self.historical_gvz.index = pd.to_datetime(self.historical_gvz.index)

            # Set the last historical GVZ date as the current date
            self.last_historical_gvz_update = actual_dt

            # If we are using the open price, then return the open price for today
            if use_open:
                # Get the row closest to today
                nearest_date = self.historical_gvz.index.asof(today_date)
                gvz_val = self.historical_gvz.loc[nearest_date]['Open']
            else:
                # Get the row closest to the previous date
                nearest_date = self.historical_gvz.index.asof(previous_date)
                gvz_val = self.historical_gvz.loc[nearest_date]['Close']

            # Add a marker to the chart to show the GVZ value
            self.strategy.add_marker(
                "gvz_value",
                value=gvz_val,
                symbol="square",
                color="blue",
            )

            return gvz_val

        except Exception as e:
            # Get the traceback
            self.strategy.log_message(f"ERROR: Failed to fetch live GVZ value: {e}", color="red", broadcast=True)
            traceback.print_exc()
            return 1000

    def get_gvz_percentile(self, dt, window, use_open=False):
        """
        Get the GVZ percentile value for the given window.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to get the GVZ percentile value.
        window : int
            The window (in days) to calculate the GVZ percentile.
        use_open : bool
            Whether to use the open price of the GVZ

        Returns
        -------
        float
            The GVZ percentile value.
        """
        # Check if dt is not None
        if dt is None:
            return None
        
        # Check if window is not None
        if window is None:
            return None

        # Get the GVZ values for the window
        gvz_values = self.get_gvz_values(dt, window, use_open=use_open)

        if gvz_values is None or len(gvz_values) == 0:
            return None

        gvz = self.get_gvz_value(dt, use_open=use_open)
        if gvz is None:
            return None

        # Get the GVZ percentile value
        gvz_percentile = stats.percentileofscore(gvz_values, gvz)

        # Add a marker to the chart
        self.strategy.add_marker(
            "gvz_percentile", symbol="square", color="blue", value=gvz_percentile, detail_text=f"GVZ percentile: {gvz_percentile}"
        )

        return gvz_percentile

    def check_max_gvz(self, dt, max_gvz, use_open=False):
        """
        Check if the GVZ is too high. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the GVZ value.
        max_gvz : float
            The maximum GVZ value to create a condor.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        bool
            True if the GVZ is too high, False otherwise.
        """
        # Get the GVZ value
        gvz = self.get_gvz_value(dt, use_open=use_open)

        # Plot the GVZ value
        if gvz is not None:
            self.strategy.add_line("gvz", gvz)
            self.strategy.add_marker("gvz", symbol="square", color="blue", detail_text=f"GVZ: {gvz}", value=gvz)

        # Check if the GVZ is greater than the maximum GVZ
        if gvz is None or max_gvz is None:
            return False
        if gvz > max_gvz:
            # Log message that the GVZ is too high
            self.strategy.log_message(
                f"GVZ is too high: {gvz} which is greater than the max of {max_gvz}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "gvz_too_high", symbol="circle", color="blue", detail_text=f"GVZ too high: {gvz}"
            )

            return True
        
        # Log message that the GVZ is within the limits
        self.strategy.log_message(
            f"GVZ is within the limits: {gvz} which is less than the max of {max_gvz}",
            color="green", broadcast=True
        )
        
        return False

    def check_min_gvz(self, dt, min_gvz, use_open=False):
        """
        Check if the GVZ is too low. If it is, log a message, add a marker to the chart, and return True.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to check the GVZ value.
        min_gvz : float
            The minimum GVZ value to create a condor.
        use_open : bool
            Whether to use the open price of the GVZ.

        Returns
        -------
        bool
            True if the GVZ is too low, False otherwise.
        """
        # Get the GVZ value
        gvz = self.get_gvz_value(dt, use_open=use_open)

        if gvz is None or min_gvz is None:
            return False
        if gvz < min_gvz:
            # Log message that the GVZ is too low
            self.strategy.log_message(
                f"GVZ is too low: {gvz} which is less than the min of {min_gvz}",
                color="yellow", broadcast=True
            )

            # Add a marker to the chart
            self.strategy.add_marker(
                "gvz_too_low", symbol="circle", color="blue", detail_text=f"GVZ too low: {gvz}"
            )

            return True
        
        return False