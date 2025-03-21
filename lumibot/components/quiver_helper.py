import os
import requests
from dotenv import load_dotenv
import datetime
import time
import pandas as pd
import io

# Load the .env file where the API key is stored
load_dotenv()

# Get the API key from the environment variable
api_token = os.getenv("QUIVER_API_KEY")

# Check if the API key is available
if api_token is None:
    raise ValueError("Quiver Quant API key not found. Please set the QUIVER_API_KEY environment variable.")

# QuiverQuant API URL
bulk_congress_trading_url = "https://api.quiverquant.com/beta/bulk/congresstrading"
bulk_congress_trading_data_csv = "bulk_congress_trading_data.csv"

"""
    Description
    -----------
    This module interacts with QuiverQuant's bulk Congress trading endpoint to fetch,
    store, and process congressional trading data for specific Bioguide IDs.
    
    The `QuiverHelper` class helps download data from QuiverQuant, caches the data in a CSV file,
    and provides methods for retrieving and calculating portfolios based on transaction histories.
"""


class QuiverHelper:
    """
    A helper class to interact with QuiverQuant's Congress trading data.

    Attributes
    ----------
    strategy : Strategy
        The parent strategy that this helper is part of.
    bulk_congress_trading_downloads : list
        A list that keeps track of dictionary objects representing 
        downloaded Congress trading data (including bioguide_id, 
        download datetime, and the list of transaction data).
    bulk_congress_trading_df : pd.DataFrame
        A pandas DataFrame that stores all downloaded Congress trading data 
        from CSV or newly fetched from QuiverQuant.
    """

    def __init__(self, strategy):
        """
        Initialize the QuiverHelper component.

        Parameters
        ----------
        strategy : Strategy
            The strategy that this component belongs to.
        
        Returns
        -------
        None
        """
        self.strategy = strategy

        # Set the initial state of the component
        # This list will store metadata about downloads for quick checks.
        self.bulk_congress_trading_downloads = [
            # Example structure:
            # {
            #     "bioguide_id": "P000197",
            #     "download_datetime": datetime.datetime.now(),
            #     "data": [
            #         {
            #             "Representative": "Nancy Pelosi",
            #             "BioGuideID": "P000197",
            #             "ReportDate": "2024-07-30",
            #             "TransactionDate": "2024-07-26",
            #             "Ticker": "NVDA",
            #             "Transaction": "Purchase",
            #             "Range": "$1,000,001 - $5,000,000",
            #             "House": "Representatives",
            #             "Amount": "1000001.0",
            #             "Party": "D",
            #             "last_modified": "2024-07-31"
            #         },
            #     ]
            # },
            # {
            #     "bioguide_id": "A000367",
            #     "download_datetime": datetime.datetime.now(),
            #     "data": []
            # }
        ]

        # Load bulk congress trading data from CSV file if it exists
        self.load_bulk_congress_trading_data()

    def load_bulk_congress_trading_data(self):
        """
        Load existing Congress trading data from a CSV file into a pandas DataFrame.
        If the file does not exist, creates an empty DataFrame with predefined columns.

        Returns
        -------
        None
        """
        # Check if the CSV file exists in the current directory
        if os.path.exists(bulk_congress_trading_data_csv):
            # Read the CSV file into a DataFrame; parse 'download_datetime' as dates
            self.bulk_congress_trading_df = pd.read_csv(
                bulk_congress_trading_data_csv,
                parse_dates=["download_datetime"]
            )
        else:
            # Create an empty DataFrame with the desired columns if CSV doesn't exist
            self.bulk_congress_trading_df = pd.DataFrame(
                columns=["bioguide_id", "download_datetime", "data"]
            )

    def fetch_congress_trading_data(self, bioguide_id, page=1, page_size=50, retries=3):
        """
        Fetch paginated Congress trading data from QuiverQuant's bulk endpoint 
        for a specific bioguide_id, handling rate limiting and retry logic.

        Parameters
        ----------
        bioguide_id : str
            The Bioguide ID of a specific representative or senator.
        page : int, optional
            The page number to fetch (defaults to 1).
        page_size : int, optional
            Number of results per page (defaults to 50).
        retries : int, optional
            Number of times to retry the request in case of certain errors (defaults to 3).

        Returns
        -------
        list
            A list of JSON-like dictionaries containing trading data for the specified page.

        Raises
        ------
        Exception
            If the data cannot be fetched successfully after the specified number of retries.
        """
        # Prepare the headers needed for authentication and data format
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json"
        }
        
        # Prepare query parameters
        params = {
            "page": page,
            "page_size": page_size,
            "bioguide_id": bioguide_id,
            "version": "V1"
        }

        # Attempt fetching up to 'retries' times
        for attempt in range(retries):
            try:
                # Send the GET request to the QuiverQuant API
                response = requests.get(
                    bulk_congress_trading_url, headers=headers, params=params
                )
                
                # If the request was unsuccessful, raise an HTTPError
                response.raise_for_status()

                # If successful, return the JSON response (list of trading data)
                return response.json()

            except requests.exceptions.HTTPError as e:
                # Specifically handle 429 Too Many Requests
                if response.status_code == 429:
                    self.strategy.log_message("Rate limit exceeded. Retrying after 60 seconds...")
                    time.sleep(60)  # Sleep 60 seconds before retry
                else:
                    # For other HTTP errors, just raise again
                    raise e
            except Exception as e:
                # Catch all other exceptions and re-raise them after printing
                self.strategy.log_message(f"An error occurred: {e}")
                raise e

        # If we've exhausted all retries, raise an exception
        raise Exception(f"Failed to fetch data after {retries} retries.")

    def get_trading_data_for_bioguide(self, bioguide_id, as_of_date):
        """
        Fetch paginated results for a specific congressperson by bioguide_id and 
        filter transactions by 'as_of_date'. Uses local caching to avoid repeated 
        downloads within a 24-hour window.

        Parameters
        ----------
        bioguide_id : str
            The Bioguide ID of the congressperson whose trading data is requested.
        as_of_date : datetime.date
            Only return transactions occurring on or before this date.

        Returns
        -------
        list
            A list of trading record dictionaries filtered by 'as_of_date'.
        """
        # Check if data for this bioguide_id already exists in our DataFrame
        existing_entry = self.bulk_congress_trading_df[
            self.bulk_congress_trading_df["bioguide_id"] == bioguide_id
        ]
        if not existing_entry.empty:
            # If found, check how long it's been since the last download
            last_download_time = existing_entry["download_datetime"].iloc[0]
            if (datetime.datetime.now() - last_download_time).days < 1:
                # If it has been less than 24 hours, use the cached data
                self.strategy.log_message(
                    f"Data for {bioguide_id} already fetched within the last 24 hours."
                )
                # The "data" column is stored as a JSON string; we read it in from a StringIO
                json_data = io.StringIO(existing_entry["data"].iloc[0])
                data = pd.read_json(json_data, typ="series").to_list()

                # Filter based on the 'as_of_date'
                data = [
                    result for result in data
                    if datetime.datetime.strptime(
                        result["TransactionDate"], "%Y-%m-%d"
                    ).date() <= as_of_date
                ]
                return data

        # If the data is older than 24 hours or not found, we fetch from the API
        page = 1
        page_size = 50
        total_results = []

        while True:
            # Fetch the data for this page
            data = self.fetch_congress_trading_data(bioguide_id, page, page_size)
            
            if not data:
                # If there is no more data, stop fetching
                break
            
            # Accumulate results
            total_results.extend(data)
            self.strategy.log_message(f"Fetched {len(data)} results from page {page}.")
            
            # Increment page to fetch the next set of results
            page += 1

        # Create a one-row DataFrame containing the new entry
        new_entry = pd.DataFrame([{
            "bioguide_id": bioguide_id,
            "download_datetime": datetime.datetime.now(),
            # Store the entire results list as a JSON string
            "data": pd.Series(total_results).to_json()
        }])

        # If there's already an entry, update it. Otherwise, append a new one.
        if not existing_entry.empty:
            # 1) Find which columns are shared by both DataFrames
            shared_cols = self.bulk_congress_trading_df.columns.intersection(new_entry.columns)
            
            # 2) Create a mask for the rows to update
            mask = self.bulk_congress_trading_df["bioguide_id"] == bioguide_id
            
            # 3) Assign only the shared columns (this avoids mismatched column errors)
            self.bulk_congress_trading_df.loc[mask, shared_cols] = (
                new_entry.loc[:, shared_cols].iloc[0].values
            )
        else:
            self.bulk_congress_trading_df = pd.concat(
                [self.bulk_congress_trading_df, new_entry],
                ignore_index=True
            )

        # Save the updated DataFrame to CSV
        self.bulk_congress_trading_df.to_csv(bulk_congress_trading_data_csv, index=False)

        # Filter total_results based on 'as_of_date'
        total_results = [
            result for result in total_results
            if datetime.datetime.strptime(
                result["TransactionDate"], "%Y-%m-%d"
            ).date() <= as_of_date
        ]

        return total_results

    def calculate_portfolio(self, transactions, as_of_date):
        """
        Calculate the portfolio of a given congressperson (or set of transactions) 
        up to a specified date. Only includes tickers with positive holdings.

        Parameters
        ----------
        transactions : list
            A list of dictionaries, each representing a single transaction 
            with keys like 'Ticker', 'TransactionDate', 'Transaction', and 'Amount'.
        as_of_date : datetime.date
            Only include transactions on or before this date.

        Returns
        -------
        dict
            A dictionary where the keys are tickers and the values are the 
            cumulative holding amounts (only if positive).
        
        Raises
        ------
        ValueError
            If 'as_of_date' is not a datetime.date object.
        """
        # Ensure the given as_of_date is indeed a datetime.date object
        if not isinstance(as_of_date, datetime.date):
            raise ValueError("as_of_date must be a datetime.date object")

        # Initialize a dictionary to keep track of holdings by ticker
        portfolio = {}

        # Go through each transaction
        for transaction in transactions:
            # Convert the string date into an actual date object
            transaction_date = datetime.datetime.strptime(
                transaction["TransactionDate"], "%Y-%m-%d"
            ).date()

            # Only consider transactions on or before the specified date
            if transaction_date <= as_of_date:
                ticker = transaction["Ticker"]
                amount = float(transaction["Amount"])
                action = transaction["Transaction"]
                
                # If the ticker is not in the portfolio yet, initialize to 0
                if ticker not in portfolio:
                    portfolio[ticker] = 0.0
                
                # Adjust holdings based on the transaction action
                if action == "Purchase":
                    portfolio[ticker] += amount
                elif action == "Sale":
                    portfolio[ticker] -= amount

        # Filter out tickers with zero or negative holdings
        portfolio = {
            ticker: holdings 
            for ticker, holdings in portfolio.items() 
            if holdings > 0
        }

        return portfolio