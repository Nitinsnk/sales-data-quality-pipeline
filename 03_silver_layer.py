# Databricks notebook source
# Cell 1: Read Bronze tables
bronze_sales = spark.table("workspace.default.bronze_sales")
bronze_products = spark.table("workspace.default.bronze_products")
bronze_customers = spark.table("workspace.default.bronze_customers")
bronze_channels = spark.table("workspace.default.bronze_channels")

print("Bronze sales rows:", bronze_sales.count())

# COMMAND ----------

# Cell 2: Clean the sales data, logging what gets dropped at each step
from pyspark.sql.functions import col, to_date, round as spark_round

step0_count = bronze_sales.count()

# Step 1: Remove exact duplicate sale_id rows (keep first occurrence)
silver_sales = bronze_sales.dropDuplicates(["sale_id"])
step1_count = silver_sales.count()
print(f"Dropped {step0_count - step1_count} duplicate sale_id rows")

# Step 2: Drop rows with nulls in critical numeric fields
silver_sales = silver_sales.filter(col("unit_price").isNotNull() & col("units_sold").isNotNull())
step2_count = silver_sales.count()
print(f"Dropped {step1_count - step2_count} rows with null unit_price/units_sold")

# Step 3: Drop rows with null channel
silver_sales = silver_sales.filter(col("channel").isNotNull())
step3_count = silver_sales.count()
print(f"Dropped {step2_count - step3_count} rows with null channel")

# Step 4: Fix data types
silver_sales = (silver_sales
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    .withColumn("units_sold", col("units_sold").cast("int"))
    .withColumn("unit_price", spark_round(col("unit_price").cast("decimal(10,2)"), 2)))

print(f"\nFinal Silver sales row count: {silver_sales.count()} (started with {step0_count})")

# COMMAND ----------

# Cell 3: Clean dimension tables (light touch — just dedupe on primary key)
silver_products = bronze_products.dropDuplicates(["product_id"])
silver_customers = bronze_customers.dropDuplicates(["customer_id"])
silver_channels = bronze_channels.dropDuplicates(["channel"])

print("Silver products:", silver_products.count())
print("Silver customers:", silver_customers.count())
print("Silver channels:", silver_channels.count())

# COMMAND ----------

# Cell 4: Write Silver Delta tables
silver_sales.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_sales")
silver_products.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_products")
silver_customers.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_customers")
silver_channels.write.format("delta").mode("overwrite").saveAsTable("workspace.default.silver_channels")

print("All 4 Silver tables written.")

# COMMAND ----------

