# Azure SQL Python ETL Pipeline with Tableau Analytics

This project is an end-to-end **Python ETL (Extract, Transform, Load) pipeline** that processes payment transaction data, stores curated analytics tables in **Azure SQL**, and visualizes key business metrics using **Tableau**.

The goal of this project is to demonstrate hands-on experience with:
- Python-based data processing
- Cloud databases (Azure SQL)
- SQL analytics modeling
- BI-ready data preparation and visualization

---

## 🚀 Project Overview

The pipeline takes raw merchant and transaction data, validates and cleans it, aggregates it into daily merchant-level metrics, and loads the results into Azure SQL using safe, idempotent SQL operations.

The final dataset is optimized for analytics and reporting, and is visualized using Tableau to show trends such as transaction volume and approval rates.

---

## 🏗️ Architecture

```text
Raw CSV Data
   ↓
Python ETL (pandas)
   ↓
Azure SQL (Fact & Dimension Tables)
   ↓
Analytics Export (CSV)
   ↓
Tableau Dashboard
```

---

## 📊 Data Model

### Dimension Table

**`dim_merchants`**
- Merchant ID
- Merchant name
- Category
- City
- State

### Fact Table

**`fact_daily_merchant_metrics`**
- Metric date (daily grain)
- Transaction count
- Approved count
- Declined count
- Gross amount
- Approved amount
- Approval rate
- Average ticket size

This structure follows common data warehousing patterns and is optimized for BI tools.

---

## 🧠 ETL Process (Python)

### 1. Extract
- Reads merchant and transaction data from CSV files using `pandas`

### 2. Validate & Clean
- Removes duplicate transactions
- Rejects invalid records:
  - Negative amounts
  - Unknown merchants
  - Invalid timestamps
- Enforces business rules before data is loaded

### 3. Transform
- Aggregates transactions into **daily merchant-level metrics**
- Calculates KPIs such as:
  - Approval rate
  - Average ticket size

### 4. Load
- Loads data into Azure SQL using:
  - Staging tables
  - SQL `MERGE` statements for safe upserts
- Ensures the pipeline is **idempotent** and safe to rerun

---

## ☁️ Azure SQL Integration

- Uses Azure SQL as the cloud data store
- Secure connection via ODBC and environment variables
- Handles firewall configuration and authentication
- Uses cost-effective DTU-based tiers

This mirrors real-world cloud data workflows.

---

## 📈 Visualization (Tableau)

Because Tableau Public does not support live database connections, the curated fact table is exported to CSV and used for visualization.

### Dashboards Created
- **Daily Gross Volume Trend**
- **Approval Rate by Merchant**

These dashboards represent common KPIs used in payment and fintech analytics.

---

## 🛠️ Tech Stack

- **Python** (pandas, SQLAlchemy, pyodbc)
- **Azure SQL**
- **SQL Server (T-SQL)**
- **Tableau Public**
- **Git & GitHub**

---

## 📂 Repository Structure

```text
azure_sql_etl/
├── data/                    # Raw input data
├── tableau/                 # Tableau-ready export
├── generate_data.py         # Synthetic data generation
├── create_tables.py         # Azure SQL schema setup
├── etl_run.py               # Main ETL pipeline
├── export_for_tableau.py    # Export curated data for Tableau
├── test_connection.py       # Azure SQL connectivity test
├── verify_load.py           # Data verification script
├── .gitignore
└── README.md
```

---

## ▶️ How to Run

### 1. Set up Azure SQL
- Create an Azure SQL database
- Configure firewall access for your IP

### 2. Configure Environment Variables
Create a `.env` file with your Azure SQL connection details.

---

### 3. Generate Sample Data

```bash
python generate_data.py
```

---

### 4. Create Database Tables

```bash
python create_tables.py
```

---

### 5. Run the ETL Pipeline

```bash
python etl_run.py
```

---

### 6. Export Data for Tableau

```bash
python export_for_tableau.py
```
