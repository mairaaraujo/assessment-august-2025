from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, trim, col
import os



CHANNEL_GROUP_INPUT_FILE_PATH = os.getenv("CHANNEL_GROUP_INPUT_FILE_PATH")
OUTPUT_PATH = "files/output/channel_groups"

def get_channel_group_schema():
    return StructType([
        StructField("TRADE_CHNL_DESC", StringType(), True),
        StructField("TRADE_GROUP_DESC", StringType(), True),
        StructField("TRADE_TYPE_DESC", StringType(), True)
    ])

def ingest_channel_group():
    spark = SparkSession.builder.appName("ChannelGroupIngestion").getOrCreate()
    schema = get_channel_group_schema()
    df = spark.read.option("header", True).schema(schema).csv(CHANNEL_GROUP_INPUT_FILE_PATH)
    for c in df.columns:
        df = df.withColumn(c, trim(regexp_replace(col(c), r'[^\x20-\x7E]', '')))
    return df

def save_channel_group_to_delta(df):
    """
    Save the channel group DataFrame to a Delta table at the specified output path.
    """
    df.write.format("parquet").mode("overwrite").save(OUTPUT_PATH)

if __name__ == "__main__":
    df_channel_group = ingest_channel_group()
    df_channel_group.show(truncate=False)
    save_channel_group_to_delta(df_channel_group)
