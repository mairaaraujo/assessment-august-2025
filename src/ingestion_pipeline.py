
import sys
import os
from pyspark.sql import SparkSession
# Add project root to Python path
sys.path.append(os.path.abspath(".."))

from pipeline.landing_channel_group import landing_channel_group
from pipeline.landing_beverage_sales import landing_beverage_sales
from pipeline.silver_channel_group import silver_channel_group
from pipeline.silver_beverage_sales import silver_beverage_sales



BEVERAGE_SALES_SILVER_PATH = os.getenv("BEVERAGE_SALES_SILVER_PATH")
CHANNEL_GROUP_SILVER_PATH = os.getenv("CHANNEL_GROUP_SILVER_PATH")

spark = SparkSession.builder \
    .appName("Ingestion pipeline") \
    .getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

landing_channel_group(spark)

landing_beverage_sales(spark)

silver_channel_group(spark)


df = silver_beverage_sales(spark)


spark.sql(f"""
CREATE TABLE IF NOT EXISTS beverage_sales
USING PARQUET
LOCATION '{BEVERAGE_SALES_SILVER_PATH}'""")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS channel_groups
USING PARQUET
LOCATION '{CHANNEL_GROUP_SILVER_PATH}'""")

spark.sql("select * from beverage_sales").show()

spark.stop()
