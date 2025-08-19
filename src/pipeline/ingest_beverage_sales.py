from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, trim, col, to_date
import os
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()
BEVERAGE_INPUT_FILE_PATH = os.getenv("BEVERAGE_INPUT_FILE_PATH")
OUTPUT_PATH = "files/output/beverage_sales/landing"

# Create a single SparkSession instance
spark = SparkSession.builder \
    .appName("BeverageSalesPipeline") \
    .getOrCreate()

# Define schema for beverage sales
def get_beverage_sales_schema():
    return StructType([
        StructField("DATE", StringType(), True),
        StructField("CE_BRAND_FLVR", StringType(), True),
        StructField("BRAND_NM", StringType(), True),
        StructField("Btlr_Org_LVL_C_Desc", StringType(), True),
        StructField("CHNL_GROUP", StringType(), True),
        StructField("TRADE_CHNL_DESC", StringType(), True),
        StructField("PKG_CAT", StringType(), True),
        StructField("Pkg_Cat_Desc", StringType(), True),
        StructField("TSR_PCKG_NM", StringType(), True),
        StructField("$ Volume", StringType(), True),
        StructField("YEAR", StringType(), True),
        StructField("MONTH", StringType(), True),
        StructField("PERIOD", StringType(), True)
    ])

# Ingest and clean beverage sales data
def ingest_beverage_sales():
    schema = get_beverage_sales_schema()
    df = spark.read.option("header", True).option("sep", "\t").schema(schema).csv(BEVERAGE_INPUT_FILE_PATH)
    # Fix BOM in DATE column if present
    for c in df.columns:
        if "DATE" in c and c != "DATE":
            df = df.withColumnRenamed(c, "DATE")
    for c in df.columns:
        df = df.withColumn(c, trim(regexp_replace(col(c), r'[^\x20-\x7E]', '')))
    return df

def convert_date_column(df, date_col="DATE"):
    """
    Convert the date column to yyyy-MM-dd format.
    """
    return df.withColumn(date_col, to_date(col(date_col), "M/d/yyyy"))

def rename_columns_snake_case(df):
    """
    Rename columns: remove special characters, convert to lower case, and use snake_case.
    """
    def to_snake_case(s):
        s = re.sub(r'[^A-Za-z0-9 ]+', '', s)  # Remove special characters
        s = s.replace(' ', '_')               # Replace spaces with underscores
        s = s.lower()                         # Convert to lower case
        return s
    new_columns = [to_snake_case(c) for c in df.columns]
    return df.toDF(*new_columns)

def save_beverage_sales_to_delta(df):
    """
    Save the beverage sales DataFrame to a Delta file, appending data and using dynamic partition overwrite mode.
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("append").partitionBy("DATE").parquet(OUTPUT_PATH)
    print("Saved beverage sales to Parquet format partitioned by DATE.")


if __name__ == "__main__":
    df_beverage_sales = ingest_beverage_sales()
    df_beverage_sales = convert_date_column(df_beverage_sales, "DATE")
    df_beverage_sales = rename_columns_snake_case(df_beverage_sales)
    save_beverage_sales_to_delta(df_beverage_sales)

    spark.stop()
