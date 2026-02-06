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

with engine.connect() as conn:
    row = conn.execute(text("SELECT GETDATE() AS now_utc, DB_NAME() AS db_name;")).mappings().one()
    print("CONNECTED SUCCESSFULLY")
    print(f"Server time: {row['now_utc']}")
    print(f"Database:    {row['db_name']}")
