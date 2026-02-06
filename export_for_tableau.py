import os
import urllib.parse
import pandas as pd
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
engine = create_engine(connect_url)

query = """
SELECT
    f.metric_date,
    f.merchant_id,
    m.merchant_name,
    m.category,
    m.city,
    m.state,
    f.txn_count,
    f.approved_txn_count,
    f.declined_txn_count,
    f.gross_amount,
    f.approved_amount,
    f.approval_rate,
    f.avg_ticket
FROM dbo.fact_daily_merchant_metrics f
JOIN dbo.dim_merchants m
  ON f.merchant_id = m.merchant_id
ORDER BY f.metric_date, m.merchant_name;
"""

df = pd.read_sql(query, engine)

os.makedirs("tableau", exist_ok=True)
output_path = "tableau/daily_merchant_metrics.csv"
df.to_csv(output_path, index=False)

print(f"✅ Exported {len(df)} rows to {output_path}")
