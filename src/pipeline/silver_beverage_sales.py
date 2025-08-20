from pyspark.sql.functions import monotonically_increasing_id
import os
from dotenv import load_dotenv

load_dotenv()

BEVERAGE_SALES_LANDING_PATH = os.getenv("BEVERAGE_SALES_LANDING_PATH")
BEVERAGE_SALES_SILVER_PATH = os.getenv("BEVERAGE_SALES_SILVER_PATH")



def rename_beverage_sales_columns(df):
    rename_map = {
        "DATE": "date",
        "CE_BRAND_FLVR": "ce_brand_flavour",
        "BRAND_NM": "brand_name",
        "Btlr_Org_LVL_C_Desc": "btlr_org_lvl_c_desc",
        "CHNL_GROUP": "trade_group_description",
        "TRADE_CHNL_DESC": "trade_channel_description",
        "PKG_CAT": "package_category",
        "Pkg_Cat_Desc": "package_category_description",
        "TSR_PCKG_NM": "tsr_package_name",
        "$ Volume": "money_volume",
        "YEAR": "year",
        "MONTH": "month",
        "PERIOD": "period"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def silver_beverage_sales(spark):
    df_sales = spark.read.parquet(BEVERAGE_SALES_LANDING_PATH)
    # df_sales = rename_beverage_sales_columns(df_sales)
    fact_sales = df_sales.withColumn("id", monotonically_increasing_id())
    fact_sales.limit(10).show()  # Displaying a sample of the data for verification
    # save_beverage_sales_to_silver(fact_sales)
    fact_sales.write.mode("append").partitionBy("DATE").parquet(BEVERAGE_SALES_SILVER_PATH)
    # fact_sales.write.mode("append").partitionBy("DATE").parquet(OUTPUT_PATH)
    print("Fact sales table saved to:", BEVERAGE_SALES_SILVER_PATH)
    return fact_sales
    