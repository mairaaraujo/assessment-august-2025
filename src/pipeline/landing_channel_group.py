from pyspark.sql.types import StructType, StructField, StringType
import os
from dotenv import load_dotenv

load_dotenv()

CHANNEL_GROUP_INPUT_FILE_PATH = os.getenv("CHANNEL_GROUP_INPUT_FILE_PATH")
CHANNEL_GROUP_LANDING_PATH = os.getenv("CHANNEL_GROUP_LANDING_PATH")

def get_channel_group_schema():
    return StructType([
        StructField("TRADE_CHNL_DESC", StringType(), True),
        StructField("TRADE_GROUP_DESC", StringType(), True),
        StructField("TRADE_TYPE_DESC", StringType(), True)
    ])

def ingest_channel_group(spark):
    schema = get_channel_group_schema()

    df = spark.read.option("header", True).schema(schema).csv(CHANNEL_GROUP_INPUT_FILE_PATH)
    return df

def save_channel_group_to_landing(df):
    df.write.mode("append").parquet(CHANNEL_GROUP_LANDING_PATH)

def landing_channel_group(spark):
    df_channel_group = ingest_channel_group(spark)
    save_channel_group_to_landing(df_channel_group)
