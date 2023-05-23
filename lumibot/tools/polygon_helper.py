import os
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import polygon
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset

WAIT_TIME = 60


def get_next_date(date, timespan, num_points):
    if timespan == "minute":
        delta = timedelta(minutes=num_points)
    elif timespan == "hour":
        delta = timedelta(hours=num_points)
    else:  # assuming 'day'
        delta = timedelta(days=num_points)

    return date + delta


def get_price_data_from_polygon(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    has_paid_subscription: bool = False,
    quote_asset: Asset = None,
):
    df_all = None
    df_csv = None

    LUMIBOT_POLYGON_CACHE_FOLDER = os.path.join(LUMIBOT_CACHE_FOLDER, "polygon")
    cache_filename = f"{asset.asset_type}_{asset.symbol}.csv"

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")

        cache_filename = f"{asset.asset_type}_{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}.csv"

    cache_file = os.path.join(LUMIBOT_POLYGON_CACHE_FOLDER, cache_filename)

    # Check if we already have data for this asset in the csv file
    if os.path.exists(cache_file):
        df_csv = pd.read_csv(cache_file, index_col="datetime")
        df_csv.index = pd.to_datetime(df_csv.index)
        df_csv = df_csv.sort_index()
        csv_start = df_csv.index[0]
        csv_end = df_csv.index[-1]

        # Check if the index is already timezone aware
        if df_csv.index.tzinfo is None:
            # Set the timezone to UTC
            df_csv.index = df_csv.index.tz_localize("UTC")

        # Check if we have data for the full range
        if csv_start <= start and csv_end >= end:
            # TODO: Also check if we are missing data in the middle of the range
            return df_csv

        # Check if we have data for the start date
        if csv_start <= start:
            cur_start = csv_end
        else:
            cur_start = start

        df_all = df_csv.copy()
    else:
        cur_start = start

    # Get the data from Polygon
    first_iteration = True
    last_cur_start = None
    while True:
        # Check if df_all exists and is not empty
        if df_all is not None and len(df_all) > 0:
            # Check if we need to get more data
            last_row = df_all.iloc[-1]
            first_row = df_all.iloc[0]

            # Check if we have all the data we need
            if last_row.name >= end and first_row.name <= start:
                # TODO: Also check if we are missing data in the middle of the range
                # We have all the data we need, break out of the loop
                break
            elif last_row.name <= cur_start:
                # Polygon doesn't have any more data for this asset, break out of the loop
                break
            # If it's an option then we need to check if the last row is past the expiration date
            elif (
                asset.asset_type == "option"
                and last_row.name.date() >= asset.expiration
                and first_row.name <= start
            ):
                # We have all the data we need, break out of the loop
                break
            else:
                # We need to get more data. Update cur_start and then get more data
                # TODO: Also check if we are missing data in the middle of the range
                if start < first_row.name:
                    cur_start = start
                else:
                    cur_start = last_row.name

                # If we don't have a paid subscription, we need to wait 1 minute between requests because of the rate limit
                if not has_paid_subscription and not first_iteration:
                    print(
                        f"""\nSleeping {WAIT_TIME} seconds getting pricing data for {asset} from Polygon because we don't have a paid subscription and we don't want to hit the rate limit. If you want to avoid this, you can get a paid subscription at https://polygon.io/pricing and set `polygon_has_paid_subscription=True` when starting the backtest.\n"""
                    )
                    time.sleep(WAIT_TIME)

        # Make sure we are not in an endless loop
        if last_cur_start is not None and last_cur_start == cur_start:
            # We already got data for this date, break out of the loop
            break
        last_cur_start = cur_start

        # If it is a crypto asset, we need to use the CryptoClient
        if asset.asset_type == "crypto":
            polygon_client = polygon.CryptoClient(api_key)

            quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
            symbol = f"X:{asset.symbol}{quote_asset_symbol}"
            result = polygon_client.get_full_range_aggregate_bars(
                symbol,
                from_date=cur_start
                - timedelta(
                    minutes=1
                ),  # We need to subtract 1 minute because of a bug in polygon
                to_date=end,
                timespan=timespan,
                run_parallel=False,
                warnings=False,
            )

            df = pd.DataFrame(result)

        elif asset.asset_type == "stock":
            polygon_client = polygon.StocksClient(api_key)

            symbol = asset.symbol
            try:
                result = polygon_client.get_full_range_aggregate_bars(
                    symbol,
                    from_date=cur_start
                    - timedelta(
                        minutes=1
                    ),  # We need to subtract 1 minute because of a bug in polygon
                    to_date=end,
                    timespan=timespan,
                    run_parallel=False,
                    warnings=False,
                )
            except Exception as e:
                print(f"Error getting data from Polygon: {e}")
                return None

            df = pd.DataFrame(result)

        elif asset.asset_type == "forex":
            polygon_client = polygon.ForexClient(api_key)

            symbol = asset.symbol
            result = polygon_client.get_full_range_aggregate_bars(
                symbol,
                from_date=cur_start
                - timedelta(
                    minutes=1
                ),  # We need to subtract 1 minute because of a bug in polygon
                to_date=end,
                timespan=timespan,
                run_parallel=False,
                warnings=False,
            )

            df = pd.DataFrame(result)

        elif asset.asset_type == "option":
            # TODO: First check if last_row.name is past the expiration date. If so, break out of the loop or something (this will save us a lot of time)

            polygon_client = polygon.OptionsClient(api_key)

            # Make asset.expiration datetime into a string like "YYMMDD"
            expiry_string = asset.expiration.strftime("%y%m%d")

            # Build option symbol
            symbol = polygon.options.options.build_option_symbol(
                asset.symbol,
                expiry_string,
                asset.right,
                asset.strike,
            )

            result = polygon_client.get_full_range_aggregate_bars(
                symbol,
                from_date=cur_start
                - timedelta(
                    minutes=1
                ),  # We need to subtract 1 minute because of a bug in polygon
                to_date=end,
                timespan=timespan,
                run_parallel=False,
                warnings=False,
            )

            df = pd.DataFrame(result)

        else:
            raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

        # Check if we got data from Polygon
        if df is not None and len(df) > 0:
            # Rename columns
            df = df.rename(
                columns={
                    "o": "open",
                    "h": "high",
                    "l": "low",
                    "c": "close",
                    "v": "volume",
                }
            )

            # Create a datetime column and set it as the index
            df = df.assign(datetime=pd.to_datetime(df["t"], unit="ms"))
            df = df.set_index("datetime")

            # Set the timezone to UTC
            df.index = df.index.tz_localize("UTC")

            if df_all is None:
                df_all = df
            else:
                df_all = pd.concat([df_all, df])

            # Sort the index
            df_all = df_all.sort_index()

        else:
            break

        first_iteration = False

    if df_all is None or len(df_all) == 0:
        return None

    # Remove any duplicate rows
    df_all = df_all[~df_all.index.duplicated(keep="first")]

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)

    # Check if df_all is different from df_csv (if df_csv exists)
    if df_csv is not None and len(df_csv) > 0:
        # Check if the dataframes are the same
        if df_csv.equals(df_all):
            # They are the same, return df_csv
            return df_csv

    # Save the data to a csv file
    df_all.to_csv(cache_file)

    return df_all
