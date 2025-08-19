from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
import re


CHANNEL_GROUP_LANDING_PATH = "files/output/channel_groups/landing"
OUTPUT_PATH = "files/output/channel_groups/silver"


spark = SparkSession.builder.appName("Landing - ChannelGroup").getOrCreate()

df_channel_group = spark.read.parquet(CHANNEL_GROUP_LANDING_PATH)

# Function to rename columns to snake_case and lower case
def rename_columns_snake_case(df):
    def to_snake_case(s):
        s = re.sub(r'[^A-Za-z0-9 ]+', '', s)
        s = s.replace(' ', '_')
        s = s.lower()
        return s
    new_columns = [to_snake_case(c) for c in df.columns]
    return df.toDF(*new_columns)

# Apply new schema for Dim_Channel
# Only keep CHNL_GROUP and TRADE_CHNL_DESC, add Channel_Key as surrogate key


df_channel_dim = df_channel_group.select("CHNL_GROUP", "TRADE_CHNL_DESC")
df_channel_dim = df_channel_dim.dropDuplicates(["CHNL_GROUP", "TRADE_CHNL_DESC"])
df_channel_dim = df_channel_dim.withColumn("channel_key", monotonically_increasing_id())
df_channel_dim = rename_columns_snake_case(df_channel_dim)


df_channel_dim = df_channel_dim.select("channel_key", "channel_group", "trade_channel_description")

# Save the dimension table
df_channel_dim.write.mode("append").parquet(OUTPUT_PATH)

print("Channel group dimension table saved to:", OUTPUT_PATH)
spark.stop()
