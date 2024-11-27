from jfx_scrapping import scrape_table_data
from BPS_scrapping import fetch_and_transform_bps_data
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_date, date_format, col, to_timestamp, row_number, lit
from pyspark.sql.window import Window
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from pyspark.sql import functions as F
from inflasi import inflasi
import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
from commodities import fetch_and_merge_futures_data
from datetime import datetime, timedelta

def fit_arima_and_predict(data, order=(1, 1, 1), steps=60):
    # Fit ARIMA model to the 'Adj Close' column
    model = ARIMA(data['Adj Close'], order=order)  # (p,d,q) order
    model_fit = model.fit()

    # Generate forecast for the next `steps` days
    forecast = model_fit.forecast(steps=steps)

    # Create a DataFrame with the forecasted values
    forecast_dates = pd.date_range(data['Date_df1'].max() + timedelta(days=1), periods=steps, freq='D')
    forecast_df = pd.DataFrame({
        'Date_df1': forecast_dates,
        'Adj Close': forecast,
        'HG=F': [None] * steps,  # Use `None` or some default value for other columns
        'GC=F': [None] * steps,
        'CL=F': [None] * steps,
        'Value': [None] * steps,
        'BI_Rate': [None] * steps
    })

    return forecast_df
# Initialize Spark session
spark = SparkSession.builder.appName("Data Collection with Time Series and Clustering").getOrCreate()

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

def generate_future_dates(last_date, num_days=60):
    # Generate the next 2 months (60 days) of dates starting from the last date
    future_dates = [last_date + timedelta(days=i) for i in range(1, num_days + 1)]
    return future_dates

from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

def collect_data():
    # 1. Fetch commodities data (1st Data)
    tickers = ['HG=F', 'GC=F', 'CL=F']
    merged_data = fetch_and_merge_futures_data(tickers, period='1y')
    spark_df_com = spark.createDataFrame(merged_data)
    spark_df_com = translate_months(spark_df_com, 'Date')
    spark_df_com = spark_df_com.withColumn('Date', to_timestamp('Date'))  # Remove timezone info
    spark_df_com = spark_df_com.withColumn('Date', to_timestamp('Date', 'dd yyyy MMMM'))  # Parse Date to timestamp
    spark_df_com = spark_df_com.withColumn('Date', date_format('Date', 'yyyy-MM-dd'))  # Format to 'YYYY-MM-DD'
    spark_df_com.show()

    # 2. BI Data (2nd Data)
    spark_df_BI = inflasi()
    spark_df_BI = spark.createDataFrame(spark_df_BI)    
    spark_df_BI = spark_df_BI.withColumn('Date', to_date(col('Date'), 'yyyy-MM'))
    spark_df_BI.show()

    # 3. BPS Data (3rd Data)
    df_result = fetch_and_transform_bps_data()
    spark_df_bps = spark.createDataFrame(df_result)
    spark_df_bps = translate_months(spark_df_bps, 'Date')
    spark_df_bps = normalize_date(spark_df_bps, 'Date', 'yyyy MMMM')
    spark_df_bps = spark_df_bps.drop(*['Time_Period'])
    spark_df_bps.show()

    # 4. Yahoo Finance Data (4th Data)
    ticker = 'IDR=X'
    data = yf.download(ticker, start="2023-01-01", end="2024-01-01", interval='1d')
    spark_df_yfinance = spark.createDataFrame(data.reset_index())
    spark_df_yfinance = spark_df_yfinance.withColumn("Date", date_format("Date", "yyyy-MM-01").cast("date"))
    spark_df_yfinance = spark_df_yfinance[['Date', 'Adj Close']]
    spark_df_yfinance.show()

    # Step 3: Join all datasets on the 'YearMonth' column using an outer join
    df1 = spark_df_com.withColumnRenamed("Date", "Date_df1")
    df2 = spark_df_BI.withColumnRenamed("Date", "Date_df2")
    df3 = spark_df_bps.withColumnRenamed("Date", "Date_df3")
    df4 = spark_df_yfinance.withColumnRenamed("Date", "Date_df4")
    
    # Create YearMonth column for all datasets
    df1 = df1.withColumn("YearMonth", F.date_format("Date_df1", "yyyy-MM"))
    df2 = df2.withColumn("YearMonth", F.date_format("Date_df2", "yyyy-MM"))
    df3 = df3.withColumn("YearMonth", F.date_format("Date_df3", "yyyy-MM"))
    df4 = df4.withColumn("YearMonth", F.date_format("Date_df4", "yyyy-MM"))

    # Perform the left joins based on YearMonth
    joined_df = df1.join(df2, "YearMonth", "left") \
                   .join(df3, "YearMonth", "left") \
                   .join(df4, "YearMonth", "left")

    # Drop the extra Date columns
    joined_df = joined_df.drop("Date_df2", "Date_df3", "Date_df4", "YearMonth")
    joined_df = joined_df.fillna({
        "HG=F": 0.0,         # For numerical columns, use 0.0 or any appropriate default value
        "GC=F": 0.0,
        "CL=F": 0.0,
        "Value": 0.0,
        "BI_Rate": 0.0,
        "Adj Close": 0.0
    })

    # Show the final cleaned data
    joined_df.show()

    # Generate the next 2 months of dates
    last_date_str = joined_df.orderBy("Date_df1", ascending=False).first()['Date_df1']
    last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
    future_dates = generate_future_dates(last_date, num_days=60)

    # Fit the ARIMA model and generate predictions
    joined_df_pd = joined_df.toPandas()  # Convert to pandas for ARIMA processing
    forecast_df = fit_arima_and_predict(joined_df_pd, steps=60)  # Forecast the next 60 days

    # Convert forecast_df back to Spark DataFrame
    forecast_spark_df = spark.createDataFrame(forecast_df)

    # Append forecast data to the original DataFrame
    final_df = joined_df.unionByName(forecast_spark_df, allowMissingColumns=True)

    # Step 4: Add TimeIndex
    final_df = final_df.withColumn("TimeIndex", row_number().over(Window.orderBy("Date_df1")))

    # Step 5: Perform Clustering
    feature_columns = ["HG=F", "GC=F", "CL=F", "Value", "BI_Rate", "Adj Close"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
    final_df = assembler.transform(final_df)

    # Perform K-means clustering (2 clusters: 0 and 1)
    kmeans = KMeans(k=2, seed=1, featuresCol="features", predictionCol="Cluster")
    kmeans_model = kmeans.fit(final_df)
    final_df = kmeans_model.transform(final_df)

    # Show the result with TimeIndex and Cluster
    final_df.select("Date_df1", "TimeIndex", "Cluster").show()

    # Convert Spark DataFrame to Pandas DataFrame for exporting to CSV
    final_result_df = final_df.toPandas()

    # Export to CSV
    final_result_df.to_csv('final_data_with_time_series_and_clustering.csv', index=False)

    # Stop Spark session
    spark.stop()

    return final_result_df
# Usage
df_final = collect_data()
print(df_final)
