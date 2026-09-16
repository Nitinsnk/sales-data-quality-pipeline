# Databricks notebook source
# Cell 1: Read Silver tables
silver_sales = spark.table("workspace.default.silver_sales")
silver_products = spark.table("workspace.default.silver_products")
silver_customers = spark.table("workspace.default.silver_customers")
silver_channels = spark.table("workspace.default.silver_channels")

print("Silver sales rows:", silver_sales.count())

# COMMAND ----------

# Cell 2: Build dim_product, dim_customer, dim_channel
from pyspark.sql.functions import monotonically_increasing_id

dim_product = silver_products.select("product_id", "product_name", "category")
dim_customer = silver_customers.select("customer_id", "customer_name", "region")

dim_channel = (silver_channels
    .withColumnRenamed("channel", "channel_name")
    .withColumn("channel_id", monotonically_increasing_id() + 1))

print("dim_product:", dim_product.count())
print("dim_customer:", dim_customer.count())
print("dim_channel:", dim_channel.count())
dim_channel.show()

# COMMAND ----------

# Cell 3: Build dim_date from distinct sale dates
from pyspark.sql.functions import col, year, month, quarter, dayofweek, date_format

dim_date = (silver_sales.select(col("sale_date").alias("date_key")).distinct()
    .withColumn("year", year("date_key"))
    .withColumn("month", month("date_key"))
    .withColumn("quarter", quarter("date_key"))
    .withColumn("day_of_week", date_format("date_key", "EEEE")))

print("dim_date:", dim_date.count())
dim_date.orderBy("date_key").show(5)

# COMMAND ----------

# Cell 4: Build fact_sales via inner join — this is where broken FKs get caught
step0_count = silver_sales.count()

fact_sales = (silver_sales
    .join(dim_product, on="product_id", how="inner")
    .join(dim_customer, on="customer_id", how="inner")
    .join(dim_channel.select("channel_name", "channel_id"), silver_sales["channel"] == dim_channel["channel_name"], how="inner")
    .select(
        "sale_id", "sale_date", "product_id", "customer_id", "channel_id",
        "units_sold", "unit_price"
    ))

step1_count = fact_sales.count()
print(f"Silver sales rows: {step0_count}")
print(f"Fact_sales rows after join: {step1_count}")
print(f"Dropped {step0_count - step1_count} rows with broken product/customer FK references")

# COMMAND ----------

# Cell 5: Write all Gold Delta tables
dim_product.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_product")
dim_customer.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_customer")
dim_channel.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_channel")
dim_date.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dim_date")
fact_sales.write.format("delta").mode("overwrite").saveAsTable("workspace.default.fact_sales")

print("All 5 Gold tables written: dim_product, dim_customer, dim_channel, dim_date, fact_sales")

# COMMAND ----------

