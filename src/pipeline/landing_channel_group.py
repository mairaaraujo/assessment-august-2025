from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os
from dotenv import load_dotenv

load_dotenv()

CHANNEL_GROUP_INPUT_FILE_PATH = os.getenv("CHANNEL_GROUP_INPUT_FILE_PATH")
OUTPUT_PATH = "files/output/channel_groups/landing"

def get_channel_group_schema():
    return StructType([
        StructField("TRADE_CHNL_DESC", StringType(), True),
        StructField("TRADE_GROUP_DESC", StringType(), True),
        StructField("TRADE_TYPE_DESC", StringType(), True)
    ])

def ingest_channel_group():
    spark = SparkSession.builder.appName("Landing - ChannelGroup").getOrCreate()
    schema = get_channel_group_schema()

    df = spark.read.option("header", True).schema(schema).csv(CHANNEL_GROUP_INPUT_FILE_PATH)
    return df

def save_channel_group_to_landing(df):
    df.write.mode("append").parquet(OUTPUT_PATH)

if __name__ == "__main__":
    df_channel_group = ingest_channel_group()
    save_channel_group_to_landing(df_channel_group)
