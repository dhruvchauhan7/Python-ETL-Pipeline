import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")

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

with engine.connect() as conn:
    merchants = conn.execute(text("SELECT COUNT(*) AS c FROM dbo.dim_merchants;")).mappings().one()["c"]
    facts = conn.execute(text("SELECT COUNT(*) AS c FROM dbo.fact_daily_merchant_metrics;")).mappings().one()["c"]

    print(f"dim_merchants rows: {merchants}")
    print(f"fact_daily_merchant_metrics rows: {facts}")

    print("\nTop 10 rows from fact table:")
    rows = conn.execute(
        text("""
            SELECT TOP 10 *
            FROM dbo.fact_daily_merchant_metrics
            ORDER BY metric_date DESC, gross_amount DESC;
        """)
    ).mappings().all()

    for r in rows:
        print(dict(r))
