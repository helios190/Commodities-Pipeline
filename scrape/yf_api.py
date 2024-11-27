import yfinance as yf
import pandas as pd

def fetch_and_merge_futures_data(tickers, period='1y'):
    ticker_to_commodity = {
    "GC=F": 1,  # Gold
    "SI=F": 2,  # Silver
    "CL=F": 3,  # Crude Oil
    "HG=F": 4,  # Copper
    "ZC=F": 5,  # Corn
    "ZW=F": 6,  # Wheat
    "NG=F": 7,  # Natural Gas
    "ZS=F": 8,  # Soybeans
    "KC=F": 9,  # Coffee
    "CC=F": 10, # Cocoa
    "LE=F": 11, # Live Cattle
    "LC=F": 12, # Lean Hogs
    "OJ=F": 13  # Orange Juice
}
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
    all_data = []
    for ticker in tickers:
        ticker_data = yf.Ticker(ticker).history(period=period)
        print(f"Available columns for {ticker}: {ticker_data.columns.tolist()}")
        columns_to_extract = ['Open', 'Close', 'High', 'Low', 'Volume']
        
        for date, row in ticker_data[columns_to_extract].iterrows():
            commodity_id = ticker_to_commodity.get(ticker, None)
            
            # Skip if commodity_id is not found
            if commodity_id is None:
                continue
            row_data = {
                'ticker': ticker,
                'date': date,
                'open': row['Open'],
                'close': row['Close'],
                'high': row['High'],
                'low': row['Low'],
                'volume': row['Volume']
            }
            # Append to all_data
            all_data.append(row_data)

    # Create a DataFrame from the collected data
    df = pd.DataFrame(all_data)
    commodity_mapping = {
        "GC=F": 1, "SI=F": 2, "CL=F": 3, "HG=F": 4, "ZC=F": 5, 
        "ZW=F": 6, "NG=F": 7, "ZS=F": 8, "KC=F": 9, "CC=F": 10, 
        "LE=F": 11, "LC=F": 12, "OJ=F": 13
    }

    # Add commodity_id column to the DataFrame
    df['commodity_id'] = df['ticker'].map(commodity_mapping)

    # Drop the ticker column since it's no longer needed
    df.drop('ticker', axis=1, inplace=True)

    # Make sure columns are in the right order
    df = df[['commodity_id', 'date', 'open', 'close', 'high', 'low', 'volume']]
    return df

tickers = [
    'GC=F',  # Gold
    'SI=F',  # Silver
    'CL=F',  # Crude Oil
    'HG=F',  # Copper
    'ZC=F',  # Corn
    'ZW=F',  # Wheat
    'NG=F',  # Natural Gas
    'ZS=F',  # Soybeans
    'KC=F',  # Coffee
    'CC=F',  # Cocoa
    'LE=F',  # Live Cattle
    'OJ=F',  # Orange Juice
    ]

df = fetch_and_merge_futures_data(tickers)
print(df)


csv_filename = 'commodities_data.csv'
df.to_csv(csv_filename, index=False)
print(f"Data exported to {csv_filename}")
