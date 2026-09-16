# Sales Data Quality & Reconciliation Pipeline

A Databricks-based data engineering pipeline that transforms messy, raw channel-sales data into a validated, analytics-ready star schema — with an automated Data Quality Scorecard checking the pipeline's own output at every stage.

## Overview

This project follows the **medallion architecture** (Bronze → Silver → Gold), a standard pattern in modern data engineering, built entirely on **Databricks** using **PySpark** and **Delta Lake**.

Rather than just building a pipeline that "works," this project focuses on the part most student projects skip: **proving the data is trustworthy** — through automated validation, referential integrity checks, and row-count reconciliation across every pipeline stage.

## Architecture

**Bronze Layer** — Raw ingestion
Reads raw CSV sales data (intentionally messy: nulls, duplicates, and orphaned foreign keys) and writes it as-is into Delta tables, preserving the original data for auditability.

**Silver Layer** — Cleaning & transformation
Deduplicates records, casts data types correctly, handles nulls, and standardizes formats using PySpark.

**Gold Layer** — Star schema
Builds a proper analytics-ready star schema:
- `fact_sales` (transaction-level fact table)
- `dim_product`, `dim_channel`, `dim_date` (dimension tables)

**Data Quality Engine** — Automated validation
A dedicated PySpark script that scores the pipeline's own output, checking:
- Null rates per critical column
- Duplicate primary keys
- Referential integrity (do all fact table foreign keys exist in their dimension tables?)
- Row-count reconciliation between Bronze → Silver → Gold

Sample result: **13 PASS checks** across nulls, duplicate keys, and referential integrity, plus row-count reconciliation tracked at each stage (12,240 → 11,110 → 10,519 rows). See `screenshots/quality_scorecard.png`.

## Tech Stack

Databricks, PySpark, Delta Lake, SQL, Git

## Repository Structure

**notebooks/**
- `01_generate_synthetic_data.py` — Creates realistic messy sales data
- `02_bronze_layer.py` — Raw data ingestion
- `03_silver_layer.py` — Cleaning and transformation
- `04_gold_layer.py` — Star schema (fact + dimension tables)
- `05_data_quality_engine.py` — Automated validation scorecard

**screenshots/**
- `quality_scorecard.png` — Sample scorecard output

## How to Run

1. Import the notebooks into a Databricks workspace (Community Edition works fine)
2. Run `01_generate_synthetic_data.py` first to create the raw dataset
3. Run notebooks 02 → 05 in order
4. View the Data Quality Scorecard output at the end of `05_data_quality_engine.py`

## Why This Project

Most data pipelines fail silently — bad data flows through and only gets noticed once it breaks a dashboard or a business decision. This project treats **data quality as a first-class part of the pipeline itself**, not an afterthought, which is a core practice in real production data engineering teams.
