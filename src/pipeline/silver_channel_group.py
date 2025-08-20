from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import os
from dotenv import load_dotenv

load_dotenv()

CHANNEL_GROUP_LANDING_PATH = os.getenv("CHANNEL_GROUP_LANDING_PATH")
CHANNEL_GROUP_SILVER_PATH = os.getenv("CHANNEL_GROUP_SILVER_PATH")


def readable_columns_names(df):
    rename_map = {
        "TRADE_CHNL_DESC": "trade_channel_description",
        "TRADE_GROUP_DESC": "trade_group_description",
        "TRADE_TYPE_DESC": "trade_type_description",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df

def save_channel_group_to_silver(df):
    df.write.mode("append").parquet(CHANNEL_GROUP_SILVER_PATH)


def silver_channel_group(spark):

    df_channel_group = spark.read.parquet(CHANNEL_GROUP_LANDING_PATH)
    df_channel_group = readable_columns_names(df_channel_group)
    df_channel_group = df_channel_group.dropDuplicates(["trade_channel_description", "trade_group_description"])
    df_channel_group = df_channel_group.withColumn("channel_key", monotonically_increasing_id())
    save_channel_group_to_silver(df_channel_group)
