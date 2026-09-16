# Databricks notebook source
# Cell 1: Load all Gold + Bronze tables we need
fact_sales = spark.table("workspace.default.fact_sales")
dim_product = spark.table("workspace.default.dim_product")
dim_customer = spark.table("workspace.default.dim_customer")
dim_channel = spark.table("workspace.default.dim_channel")
dim_date = spark.table("workspace.default.dim_date")

bronze_sales = spark.table("workspace.default.bronze_sales")
silver_sales = spark.table("workspace.default.silver_sales")

scorecard_rows = []  # we'll append (check_name, status, details) tuples here

# COMMAND ----------

# Cell 2: Check 1 — Null rate per critical column in fact_sales
from pyspark.sql.functions import col, sum as spark_sum, when, count

critical_cols = ["sale_id", "product_id", "customer_id", "channel_id", "units_sold", "unit_price"]
total_rows = fact_sales.count()

for c in critical_cols:
    null_count = fact_sales.filter(col(c).isNull()).count()
    null_pct = round((null_count / total_rows) * 100, 2)
    status = "PASS" if null_pct <= 1.0 else "FAIL"
    scorecard_rows.append((f"Null rate: {c}", status, f"{null_count} nulls ({null_pct}%) out of {total_rows} rows"))
    print(f"[{status}] {c}: {null_count} nulls ({null_pct}%)")

# COMMAND ----------

# Cell 3: Check 2 — Duplicate primary keys
def check_duplicates(df, key_col, table_name):
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    dupes = total - distinct
    status = "PASS" if dupes == 0 else "FAIL"
    scorecard_rows.append((f"Duplicate PK: {table_name}", status, f"{dupes} duplicate {key_col} values found"))
    print(f"[{status}] {table_name}: {dupes} duplicate {key_col} values")

check_duplicates(fact_sales, "sale_id", "fact_sales")
check_duplicates(dim_product, "product_id", "dim_product")
check_duplicates(dim_customer, "customer_id", "dim_customer")
check_duplicates(dim_channel, "channel_id", "dim_channel")

# COMMAND ----------

# Cell 4: Check 3 — Referential integrity (fact vs dimensions)
def check_referential_integrity(fact_df, fk_col, dim_df, pk_col, dim_name):
    orphans = fact_df.join(dim_df, fact_df[fk_col] == dim_df[pk_col], "left_anti").count()
    status = "PASS" if orphans == 0 else "FAIL"
    scorecard_rows.append((f"Referential integrity: {fk_col} -> {dim_name}", status, f"{orphans} orphan rows in fact_sales with no matching {dim_name}"))
    print(f"[{status}] {fk_col} -> {dim_name}: {orphans} orphan rows")

check_referential_integrity(fact_sales, "product_id", dim_product, "product_id", "dim_product")
check_referential_integrity(fact_sales, "customer_id", dim_customer, "customer_id", "dim_customer")
check_referential_integrity(fact_sales, "channel_id", dim_channel, "channel_id", "dim_channel")

# COMMAND ----------

# Cell 5: Check 4 — Row-count reconciliation across Bronze -> Silver -> Gold
bronze_count = bronze_sales.count()
silver_count = silver_sales.count()
gold_count = fact_sales.count()

dropped_bronze_to_silver = bronze_count - silver_count
dropped_silver_to_gold = silver_count - gold_count

scorecard_rows.append(("Row count reconciliation: Bronze->Silver", "INFO",
                        f"{bronze_count} -> {silver_count} ({dropped_bronze_to_silver} dropped: dupes/nulls)"))
scorecard_rows.append(("Row count reconciliation: Silver->Gold", "INFO",
                        f"{silver_count} -> {gold_count} ({dropped_silver_to_gold} dropped: broken FKs)"))

print(f"Bronze: {bronze_count} -> Silver: {silver_count} -> Gold: {gold_count}")
print(f"Dropped in Silver (dupes/nulls): {dropped_bronze_to_silver}")
print(f"Dropped in Gold (broken FKs): {dropped_silver_to_gold}")

# COMMAND ----------

# Cell 6: Build and save the final Quality Scorecard
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp

schema = StructType([
    StructField("check_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("details", StringType(), True),
])

scorecard_df = spark.createDataFrame(scorecard_rows, schema).withColumn("checked_at", current_timestamp())

scorecard_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.dq_scorecard")

print("\n=== DATA QUALITY SCORECARD ===")
display(scorecard_df)

# COMMAND ----------

