from jfx_scrapping import scrape_table_data
from BI_scrapping import download_inflation_data
from BPS_scrapping import fetch_and_transform_bps_data
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_date, date_format

# Initialize Spark session
spark = SparkSession.builder.appName("Data Collection").getOrCreate()

# Helper function to replace Indonesian month names with English
indonesian_to_english_months = {
    'Januari': 'January', 'Februari': 'February', 'Maret': 'March', 'April': 'April',
    'Mei': 'May', 'Juni': 'June', 'Juli': 'July', 'Agustus': 'August',
    'September': 'September', 'Oktober': 'October', 'November': 'November', 'Desember': 'December'
}

def translate_months(spark_df, column):
    for indo_month, eng_month in indonesian_to_english_months.items():
        spark_df = spark_df.withColumn(column, expr(f"regexp_replace({column}, '{indo_month}', '{eng_month}')"))
    return spark_df

def normalize_date(spark_df, date_column, format_str):
    return spark_df.withColumn('Date', to_date(expr(f"date_format(to_date(`{date_column}`, '{format_str}'), 'yyyy-MM-01')")))

def collect_data():
    # 1. JFX Data (1st Data)
    urls = [
        'https://www.jfx.co.id/media?hal=hm-overview-harga&id=CC5',
        'https://www.jfx.co.id/media?hal=hm-overview-harga&id=GOL',
        'https://www.jfx.co.id/media?hal=hm-overview-harga&id=ACF',
        'https://www.jfx.co.id/media?hal=hm-overview-harga&id=RCF',
        'https://www.jfx.co.id/media?hal=hm-overview-harga&id=OLE'
    ]
    start_date = '01-01-2023'
    end_date = '01-01-2024'
    df_jfx = scrape_table_data(urls, start_date, end_date)
    spark_df_jfx = spark.createDataFrame(df_jfx).withColumnRenamed('BUSINESS DATE', 'Date')
    spark_df_jfx = normalize_date(spark_df_jfx, 'Date', 'dd-MM-yyyy')


    # 2. BI Data (2nd Data)
    start_date = '01/2023'
    end_date = '01/2024'
    df_BI = download_inflation_data(start_date, end_date)
    spark_df_BI = spark.createDataFrame(df_BI).withColumnRenamed('Periode', 'Date')
    spark_df_BI = translate_months(spark_df_BI, 'Date')
    spark_df_BI = normalize_date(spark_df_BI, 'Date', 'MMMM yyyy')

    # 3. BPS Data (3rd Data)
    df_result = fetch_and_transform_bps_data()
    spark_df_bps = spark.createDataFrame(df_result)
    spark_df_bps = translate_months(spark_df_bps, 'Date')
    spark_df_bps = normalize_date(spark_df_bps, 'Date', 'yyyy MMMM')
    spark_df_BI = spark_df_BI.drop(*['No'])

    # 4. Yahoo Finance Data (4th Data)
    ticker = 'IDR=X'
    data = yf.download(ticker, start="2023-01-01", end="2024-01-01", interval='1d')
    spark_df_yfinance = spark.createDataFrame(data.reset_index())
    # Standardize 'Date' to have 'YYYY-MM-01' format for monthly alignment
    spark_df_yfinance = spark_df_yfinance.withColumn("Date", date_format("Date", "yyyy-MM-01").cast("date"))
    spark_df_yfinance = spark_df_yfinance[['Date','Adj Close']]

    # Step 3: Join all datasets on the 'Date' column using an outer join
    joined_data = (
        spark_df_jfx
        .join(spark_df_BI, on="Date", how="outer")
        .join(spark_df_bps, on="Date", how="outer")
        .join(spark_df_yfinance, on="Date", how="outer")
    )

    # Step 4: Drop rows with any null values across all columns
    joined_data = joined_data.dropna()

    # Show final cleaned data
    joined_data.show()

    # Convert to pandas if needed for further processing
    final_result_df = joined_data.toPandas()

    # Stop Spark session
    spark.stop()

    return final_result_df

# Usage
df_final = collect_data()
print(df_final)
