import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col
from pyspark.sql.types import StructType, StructField, StringType


def load_file_paths():
    load_dotenv()
    channel_group_file_path = os.getenv("CHANNEL_GROUP_INPUT_FILE_PATH", "files/input/abi_bus_case1_beverage_channel_group_20210726.csv")
    beverage_file_path = os.getenv("BEVERAGE_INPUT_FILE_PATH", "files/input/abi_bus_case1_beverage_sales_20210726.csv")
    return channel_group_file_path, beverage_file_path

CHANNEL_GROUP_FILE_PATH, BEVERAGE_FILE_PATH = load_file_paths()

channel_group_schema = StructType([
    StructField("TRADE_CHNL_DESC", StringType(), True),
    StructField("TRADE_GROUP_DESC", StringType(), True),
    StructField("TRADE_TYPE_DESC", StringType(), True)
])

beverage_sales_schema = StructType([
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

def get_spark_session():
    return SparkSession.builder.appName("CSVReader").getOrCreate()

def read_and_clean_csv(path, schema, sep=",", handle_date_bom=True):
    spark = get_spark_session()
    df = spark.read.option("header", True).schema(schema).option("sep", sep).csv(path)
    if handle_date_bom:
        for c in df.columns:
            if "DATE" in c and c != "DATE":
                df = df.withColumnRenamed(c, "DATE")
    for c in df.columns:
        df = df.withColumn(c, trim(regexp_replace(col(c), r'[^\x20-\x7E]', '')))
    return df

def read_and_clean_channel_group():
    return read_and_clean_csv(CHANNEL_GROUP_FILE_PATH, channel_group_schema, sep=",", handle_date_bom=False)

def read_and_clean_beverage_sales():
    return read_and_clean_csv(BEVERAGE_FILE_PATH, beverage_sales_schema, sep="\t", handle_date_bom=True)

def join_beverage_with_channel(df_beverage, df_channel):
    return df_beverage.join(df_channel, df_beverage.TRADE_CHNL_DESC == df_channel.TRADE_CHNL_DESC, "inner")
