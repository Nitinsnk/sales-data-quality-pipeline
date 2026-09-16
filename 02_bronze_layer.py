# Databricks notebook source
# Cell 1: Read raw CSVs with PySpark and write as Bronze Delta tables
from pyspark.sql.functions import current_timestamp, lit

base_path = "/Volumes/workspace/default/raw_sales_data"

def load_bronze(file_name, table_name):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{base_path}/{file_name}")
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", lit(file_name)))

    df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.default.{table_name}")
    print(f"{table_name}: {df.count()} rows written")
    return df

bronze_sales_df = load_bronze("sales_raw.csv", "bronze_sales")
bronze_products_df = load_bronze("products.csv", "bronze_products")
bronze_customers_df = load_bronze("customers.csv", "bronze_customers")
bronze_channels_df = load_bronze("channels.csv", "bronze_channels")

# COMMAND ----------

# Cell 2: Quick sanity check
display(bronze_sales_df.limit(10))

# COMMAND ----------

