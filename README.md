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

Outputs a pass/fail **Data Quality Scorecard** (see `screenshots/quality_scorecard.png`).

## Tech Stack

Databricks, PySpark, Delta Lake, SQL, Git

## Repository Structure
