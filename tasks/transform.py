from pyspark.sql.functions import col, udf, concat_ws, to_date, when
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# -- Helper Functions for Mappings --
# Mapping for Indonesian months to English months
month_map = {
    "Januari": "January", "Februari": "February", "Maret": "March", "April": "April",
    "Mei": "May", "Juni": "June", "Juli": "July", "Agustus": "August", "September": "September",
    "Oktober": "October", "November": "November", "Desember": "December"
}

# UDF for converting Indonesian months to English
@udf(StringType())
def map_month(month):
    return month_map.get(month, None)

# -- Transform Inflation Scrape --
def transform_inflation_scrape(df,spark):
    """
    Transforms the inflation data by reshaping and cleaning it.
    """
    df = spark.createDataFrame(df)
    df_long = df.selectExpr(
    "Bulan", 
    "stack(2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024) as (Year, Value)"
)
    df_transposed = df_long.withColumn("Month", map_month(col("Bulan"))) \
                       .withColumn("Year-Date", concat_ws('-', col("Year"), col("Month"))) \
                       .withColumn("Date", to_date(col("Year-Date"), "yyyy-MMMM")) \
                       .withColumn("Year-Month", col("Date").cast("string")) \
                       .drop("Year-Date") \
                       .select("Year-Month", "Value")

    # Clean 'Value' column: Replace empty strings with 0, and convert to float
    df_transposed = df_transposed.withColumn("Value", when(col("Value") == "", 0).otherwise(col("Value")).cast("float"))

    # Return final DataFrame
    return df_transposed

# Register UDF
def convert_time_period_udf(data, time_period):
    tahun_mapping = {str(item["val"]): item["label"] for item in data.get("tahun", [])}
    turtahun_mapping = {str(item["val"]): item["label"] for item in data.get("turtahun", [])}
    year_code = time_period[5:8]
    month_code = time_period[8:]
    year = tahun_mapping.get(year_code, "Unknown Year")
    month = turtahun_mapping.get(month_code, "Unknown Month")
    return f"{year} {month}" if year != "Unknown Year" and month != "Unknown Month" else None


# Register UDF with Spark
convert_time_period_spark_udf = udf(lambda time_period: convert_time_period_udf(data, time_period), StringType())
schema = StructType([
    StructField("Time_Period", StringType(), True),
    StructField("BI_Rate", DoubleType(), True)
])


def transform_bps_scrape(data, spark):
    """
    Transforms BPS data by converting the time periods and structuring the DataFrame.
    """
    # Get data and convert to a list of tuples (key, value)
    datacontent = data.get('datacontent', {})
    data_list = list(datacontent.items())

    # Create DataFrame with the defined schema
    df = spark.createDataFrame(data_list, schema)

    # Ensure BI_Rate is cast to DoubleType for consistency
    df = df.withColumn("BI_Rate", df["BI_Rate"].cast(DoubleType()))

    # Apply the UDF to transform 'Time_Period' to 'Date'
    df_transformed = df.withColumn("Date", convert_time_period_spark_udf(col("Time_Period")))

    # Filter out rows where 'Time_Period' ends with "13"
    df_filtered = df_transformed.filter(~df_transformed["Time_Period"].rlike("13$"))

    # Return the transformed and filtered DataFrame
    return df_filtered