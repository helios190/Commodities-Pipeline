from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your scraping functions globally for better readability
from scrape.yf_api import fetch_and_merge_futures_data
from scrape.bps_extract_transform import fetch_bi_rate
from scrape.inflasi_extract_transform import fetch_inflasi
from tasks.transform import main
from tasks.load import main_load

month_map = {
    "Januari": "January", "Februari": "February", "Maret": "March", "April": "April",
    "Mei": "May", "Juni": "June", "Juli": "July", "Agustus": "August", "September": "September",
    "Oktober": "October", "November": "November", "Desember": "December"
}

# Define your Python functions for each step
def scrape_commodity_data():
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
        'LC=F',  # Lean Hogs
        'OJ=F',  # Orange Juice
    ]
    fetch_and_merge_futures_data(tickers)
    print("Commodity data scraped successfully.")

def scrape_bi_rate():
    bps_data = fetch_bi_rate()
    print("BI Rate data scraped successfully.")
    return bps_data

def scrape_inflation_rate():
    inflation_data = fetch_inflasi()
    print("Inflation rate data scraped successfully.")
    return inflation_data

def transform_data(inflation_data, bps_data):
    main(inflation_data, bps_data)

def load_data(bi_rate_data, inflasi_data, yf_data):
    main_load(bi_rate_data, inflasi_data, yf_data)
    print("Data loaded into database successfully.")

# DAG configuration
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='commodity_prices_pipeline',
    default_args=default_args,
    description='ETL pipeline for commodity prices dashboard',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Define Tasks
    scrape_commodity = PythonOperator(
        task_id='scrape_commodity_data',
        python_callable=scrape_commodity_data,
    )

    scrape_bi = PythonOperator(
        task_id='scrape_bi_rate',
        python_callable=scrape_bi_rate,
    )

    scrape_inflation = PythonOperator(
        task_id='scrape_inflation_rate',
        python_callable=scrape_inflation_rate,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,  # This allows passing context (including current date)
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Define Task Dependencies
    [scrape_commodity, scrape_bi, scrape_inflation] >> transform >> load

