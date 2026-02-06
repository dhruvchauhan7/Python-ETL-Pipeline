import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

if not all([server, database, username, password]):
    raise ValueError("Missing one or more required env vars. Check your .env file.")

odbc_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

connect_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)
engine = create_engine(connect_url, pool_pre_ping=True)

DDL = """
-- 1) Merchants dimension
IF OBJECT_ID('dbo.dim_merchants', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_merchants (
        merchant_id      VARCHAR(64)  NOT NULL PRIMARY KEY,
        merchant_name    VARCHAR(255) NOT NULL,
        category         VARCHAR(100) NULL,
        city             VARCHAR(100) NULL,
        state            VARCHAR(50)  NULL,
        created_at_utc   DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

-- 2) Daily metrics fact table (Tableau-friendly)
IF OBJECT_ID('dbo.fact_daily_merchant_metrics', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_daily_merchant_metrics (
        metric_date              DATE        NOT NULL,
        merchant_id              VARCHAR(64) NOT NULL,
        txn_count                INT         NOT NULL,
        approved_txn_count       INT         NOT NULL,
        declined_txn_count       INT         NOT NULL,
        gross_amount             DECIMAL(18,2) NOT NULL,
        approved_amount          DECIMAL(18,2) NOT NULL,
        approval_rate            DECIMAL(9,4)  NOT NULL,  -- 0.0000 to 1.0000
        avg_ticket               DECIMAL(18,2) NOT NULL,
        loaded_at_utc            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_daily PRIMARY KEY (metric_date, merchant_id),
        CONSTRAINT FK_fact_daily_merchants FOREIGN KEY (merchant_id)
            REFERENCES dbo.dim_merchants(merchant_id)
    );
END;
"""

with engine.begin() as conn:
    conn.execute(text(DDL))
    print("✅ Tables created (or already existed): dbo.dim_merchants, dbo.fact_daily_merchant_metrics")
