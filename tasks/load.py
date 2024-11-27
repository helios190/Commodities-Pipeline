from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from scrape.bps_extract_transform import fetch_bi_rate
from scrape.inflasi_extract_transform import fetch_inflasi
from scrape.yf_api import fetch_and_merge_futures_data

# Ensure the database connection is established
def test_db_connection():
    try:
        conn = psycopg2.connect("dbname=commodity-dashboard user=postgres password=Creamy123 host=localhost")
        cur = conn.cursor()
        print("You are connected to the database.")
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        print("Database error:", e)

# Function to load BI Rate data
def load_bi_rate(df):
    try:
        # Use SQLAlchemy to connect to the PostgreSQL database
        database_url = "postgresql://postgres:Creamy123@localhost:5432/commodity-dashboard"
        engine = create_engine(database_url)

        # Use the engine to load the dataframe into the 'bi_rate' table
        with engine.connect() as conn:
            df.to_sql('bi_rate', con=conn, if_exists='replace', index=False)

        print("Data successfully loaded into the 'bi_rate' table.")
    except Exception as e:
        print(f"Error loading data into 'bi_rate': {e}")

# Function to load Inflation Rate data
def load_inflasi(df_result):
    try:
        # Use SQLAlchemy to connect to the PostgreSQL database
        database_url = "postgresql://postgres:Creamy123@localhost:5432/commodity-dashboard"
        engine = create_engine(database_url)

        # Use the engine to load the dataframe into the 'inflasi_rate' table
        with engine.connect() as conn:
            df_result.to_sql('inflasi_rate', con=conn, if_exists='replace', index=False)

        print("Data successfully loaded into the 'inflasi_rate' table.")
    except Exception as e:
        print(f"Error loading data into 'inflasi_rate': {e}")

# Function to load Commodity Data (Yahoo Finance)
def load_yf(df):
    try:
        # Use SQLAlchemy to connect to the PostgreSQL database
        database_url = "postgresql://postgres:Creamy123@localhost:5432/commodity-dashboard"
        engine = create_engine(database_url)

        # Use the engine to load the dataframe into the 'yf_rate' table (Corrected table name)
        with engine.connect() as conn:
            df.to_sql('yf_rate', con=conn, if_exists='replace', index=False)

        print("Data successfully loaded into the 'yf_rate' table.")
    except Exception as e:
        print(f"Error loading data into 'yf_rate': {e}")

def main_load(bi_rate_data, inflasi_data, yf_data):
    # Step 1: Test database connection
    test_db_connection()

    # Step 2: Fetch BI Rate data
    print("Fetching BI Rate data...")
    load_bi_rate(bi_rate_data)  # Load BI Rate data into the database

    # Step 3: Fetch Inflation Rate data
    print("Fetching Inflation Rate data...")
    load_inflasi(inflasi_data)  # Load Inflation data into the database

    # Step 4: Fetch and merge Commodity data from Yahoo Finance
    print("Fetching and merging Commodity data from Yahoo Finance...")
    load_yf(yf_data)  # Load Yahoo Finance data into the database

    # Step 5: Optional - Additional processing can be added here (e.g., cleaning, transforming)

    print("All data has been successfully fetched and loaded.")
