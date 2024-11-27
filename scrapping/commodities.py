import yfinance as yf
import pandas as pd

def fetch_and_merge_futures_data(tickers, period='1y'):
    """
    Fetches the 'Open' prices for the specified tickers over the given period
    and merges them into a single DataFrame.

    Args:
    - tickers (list): List of ticker symbols (e.g., ['HG=F', 'GC=F', 'CL=F']).
    - period (str): The period of historical data to fetch. Default is '1y' (1 year).

    Returns:
    - pd.DataFrame: A DataFrame with the merged 'Open' prices for each ticker.
    """
    # Fetch the data for each ticker
    data = {}
    for ticker in tickers:
        data[ticker] = yf.Ticker(ticker).history(period=period)['Open']

    # Merge the data into a single DataFrame
    merged_data = pd.DataFrame(data)
    merged_data.reset_index(inplace=True)
    return merged_data


