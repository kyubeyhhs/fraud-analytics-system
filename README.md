# End-to-End Fraud Analytics Warehouse & BI System

## Project Overview
This project implements a professional-grade fraud detection pipeline. It transforms raw transaction and IP metadata into a structured Data Warehouse (DuckDB) and an interactive Power BI Executive Dashboard.

**Key Achievements:**
* **ETL Pipeline:** Built a Python-based ETL process that sanitizes floating-point IP noise, datetime standardization and handles 150k+ records.
* **Warehouse Architecture:** Implemented a **Star Schema** in DuckDB to optimize analytical query performance.
* **Geospatial Intelligence:** Engineered an Inequality Join to map IP addresses to countries with 85%+ coverage.
* **Business Logic:** Developed DAX measures for Fraud Rate, Chargeback Recovery, and Alert Accuracy.

## Tech Stack
* **Language:** Python (Pandas, NumPy)
* **Database:** DuckDB (In-process OLAP)
* **BI Tool:** Power BI Desktop
* **Data Model:** Star Schema (1 Fact, 3 Dimensions)

## The Data Model
The system uses a centralized **Fact Table** (`f_transactions`) connected to:
1. **d_calendar:** For Time-Intelligence and MoM growth analysis.
2. **d_alerts:** For operational workflow tracking.
3. **d_chargebacks:** For financial loss mitigation.

## Dashboard Highlights
* **Executive Overview:** Real-time monitoring of Fraud Rate, Value and Source.
* **Operational Analysis:** Heatmap of high-velocity fraud, Ongoing Alerts, Accuracy and Chargeback Winrate.
* **Data Validation:** A dedicated integrity page monitoring IP mapping coverage and timestamp logic.

## Setup Instructions
1. Clone the repo.
2. Install dependencies: `pip install -r requirements.txt`
3. Run `python scripts/generate_pipeline.py` to generate the DuckDB warehouse and BI exports or `python scripts/ad-hoc.py` for quick validation and check.
4. Open `dashboard/fraud_analysis.pbix` to view the analysis.