# Databricks notebook source
# DBTITLE 1,# Cell 1: Install Faker (Free Edition doesn't have it pre-installed) %pip install faker
# Cell 1: Install Faker (Free Edition doesn't have it pre-installed)
%pip install faker

# COMMAND ----------

# Cell (run once after installing faker)
dbutils.library.restartPython()

# COMMAND ----------

import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

fake = Faker()
random.seed(42)
Faker.seed(42)

channels = ["Wholesale", "Dealership", "Online", "Retail Partner"]

products = []
for i in range(1, 31):
    pid = "P" + str(i).zfill(3)
    pname = fake.word().capitalize() + " " + random.choice(["Sedan", "SUV", "Truck", "Coupe", "Hatchback"])
    category = random.choice(["Compact", "Luxury", "Commercial", "Electric"])
    products.append({"product_id": pid, "product_name": pname, "category": category})

customers = []
for i in range(1, 201):
    cid = "C" + str(i).zfill(4)
    cname = fake.company()
    region = fake.state()
    customers.append({"customer_id": cid, "customer_name": cname, "region": region})

start_date = datetime(2023, 1, 1)
end_date = datetime(2026, 9, 1)

def random_date():
    delta = end_date - start_date
    return start_date + timedelta(days=random.randint(0, delta.days))

rows = []
n_rows = 12000
for i in range(n_rows):
    product = random.choice(products)
    customer = random.choice(customers)
    row = {}
    row["sale_id"] = "S" + str(i).zfill(6)
    row["sale_date"] = random_date().strftime("%Y-%m-%d")
    row["product_id"] = product["product_id"]
    row["customer_id"] = customer["customer_id"]
    row["channel"] = random.choice(channels)
    row["units_sold"] = random.randint(1, 50)
    row["unit_price"] = round(random.uniform(15000, 85000), 2)

    r = random.random()
    if r < 0.03:
        row["product_id"] = "P" + str(random.randint(900, 999))
    elif r < 0.05:
        row["customer_id"] = "C" + str(random.randint(9000, 9999))
    elif r < 0.08:
        row["unit_price"] = None
    elif r < 0.10:
        row["units_sold"] = None
    elif r < 0.12:
        row["channel"] = None

    rows.append(row)

sales_df = pd.DataFrame(rows)
dupes = sales_df.sample(frac=0.02, random_state=1).copy()
sales_df = pd.concat([sales_df, dupes], ignore_index=True)

products_df = pd.DataFrame(products)
customers_df = pd.DataFrame(customers)
channels_df = pd.DataFrame({"channel": channels})

print("Sales rows:", len(sales_df))
print("Products:", len(products_df), "Customers:", len(customers_df))
sales_df.head()

# COMMAND ----------

base_path = "/Volumes/workspace/default/raw_sales_data"

sales_df.to_csv(f"{base_path}/sales_raw.csv", index=False)
products_df.to_csv(f"{base_path}/products.csv", index=False)
customers_df.to_csv(f"{base_path}/customers.csv", index=False)
channels_df.to_csv(f"{base_path}/channels.csv", index=False)

print("Saved 4 CSV files to:", base_path)