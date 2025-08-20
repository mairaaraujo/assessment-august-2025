from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, trim, col, to_date
import os
import re
from dotenv import load_dotenv

load_dotenv()

BEVERAGE_INPUT_FILE_PATH = os.getenv("BEVERAGE_INPUT_FILE_PATH")
BEVERAGE_SALES_LANDING_PATH = os.getenv("BEVERAGE_SALES_LANDING_PATH")

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

def ingest_beverage_sales(spark):
    schema = get_beverage_sales_schema()
    df = spark.read.option("header", True).option("sep", "\t").schema(schema).csv(BEVERAGE_INPUT_FILE_PATH)

    # Clean column names - remove BOM and special characters
    for c in df.columns:
        if "DATE" in c and c != "DATE":
            df = df.withColumnRenamed(c, "DATE")
    for c in df.columns:
        df = df.withColumn(c, trim(regexp_replace(col(c), r'[^\x20-\x7E]', '')))
    return df


def convert_date_column(df, date_col="DATE"):
    return df.withColumn(date_col, to_date(col(date_col), "M/d/yyyy"))

def save_beverage_sales_to_landing(df):
    df.write.mode("append").partitionBy("DATE").parquet(BEVERAGE_SALES_LANDING_PATH)
    print("Saved beverage sales to Parquet format partitioned by DATE.")


def landing_beverage_sales(spark):
    df_beverage_sales = ingest_beverage_sales(spark)
    df_beverage_sales = convert_date_column(df_beverage_sales, "DATE")
    save_beverage_sales_to_landing(df_beverage_sales)
    df_beverage_sales.limit(10).show()  # Displaying a sample of the data for verification
