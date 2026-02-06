import os
import urllib.parse
from dataclasses import dataclass
from typing import Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# -----------------------------
# Config / DB Engine
# -----------------------------
def get_engine():
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
    return create_engine(connect_url, pool_pre_ping=True)


@dataclass
class RunStats:
    txns_total: int = 0
    txns_after_dedupe: int = 0
    txns_valid: int = 0
    txns_rejected: int = 0
    merchants_loaded: int = 0
    daily_rows: int = 0
    fact_rows_upserted: int = 0


# -----------------------------
# Extract
# -----------------------------
def extract() -> Tuple[pd.DataFrame, pd.DataFrame]:
    merchants = pd.read_csv("data/merchants.csv", dtype=str)
    txns = pd.read_csv("data/transactions.csv", dtype=str)
    return merchants, txns


# -----------------------------
# Validate / Clean
# -----------------------------
def validate_and_clean(merchants: pd.DataFrame, txns: pd.DataFrame) -> Tuple[pd.DataFrame, RunStats]:
    stats = RunStats()
    stats.txns_total = len(txns)

    # Normalize columns
    merchants.columns = [c.strip() for c in merchants.columns]
    txns.columns = [c.strip() for c in txns.columns]

    required_merch_cols = {"merchant_id", "merchant_name"}
    required_txn_cols = {"transaction_id", "merchant_id", "txn_ts_utc", "amount", "status"}

    if not required_merch_cols.issubset(set(merchants.columns)):
        missing = required_merch_cols - set(merchants.columns)
        raise ValueError(f"merchants.csv missing columns: {missing}")

    if not required_txn_cols.issubset(set(txns.columns)):
        missing = required_txn_cols - set(txns.columns)
        raise ValueError(f"transactions.csv missing columns: {missing}")

    # Strip whitespace
    for c in merchants.columns:
        merchants[c] = merchants[c].astype(str).str.strip()

    for c in txns.columns:
        txns[c] = txns[c].astype(str).str.strip()

    # Dedupe by transaction_id (keep first)
    txns = txns.drop_duplicates(subset=["transaction_id"], keep="first")
    stats.txns_after_dedupe = len(txns)

    # Parse types
    txns["amount_num"] = pd.to_numeric(txns["amount"], errors="coerce")
    txns["txn_ts"] = pd.to_datetime(txns["txn_ts_utc"], errors="coerce", utc=True)

    # Validations
    valid_status = txns["status"].isin(["APPROVED", "DECLINED"])
    valid_amount = txns["amount_num"].notna() & (txns["amount_num"] > 0)
    valid_ts = txns["txn_ts"].notna()

    merchant_ids = set(merchants["merchant_id"].dropna().astype(str))
    valid_merchant = txns["merchant_id"].isin(merchant_ids)

    is_valid = valid_status & valid_amount & valid_ts & valid_merchant

    clean_txns = txns.loc[is_valid].copy()

    stats.txns_valid = len(clean_txns)
    stats.txns_rejected = len(txns) - len(clean_txns)

    return clean_txns, stats


# -----------------------------
# Transform (daily merchant metrics)
# -----------------------------
def transform_daily_metrics(clean_txns: pd.DataFrame) -> pd.DataFrame:
    # metric_date in UTC (date part)
    clean_txns["metric_date"] = clean_txns["txn_ts"].dt.date

    clean_txns["is_approved"] = (clean_txns["status"] == "APPROVED").astype(int)
    clean_txns["is_declined"] = (clean_txns["status"] == "DECLINED").astype(int)

    clean_txns["approved_amount"] = clean_txns["amount_num"].where(clean_txns["status"] == "APPROVED", 0.0)

    agg = (
        clean_txns.groupby(["metric_date", "merchant_id"], as_index=False)
        .agg(
            txn_count=("transaction_id", "count"),
            approved_txn_count=("is_approved", "sum"),
            declined_txn_count=("is_declined", "sum"),
            gross_amount=("amount_num", "sum"),
            approved_amount=("approved_amount", "sum"),
        )
    )

    # Derived metrics
    agg["approval_rate"] = (agg["approved_txn_count"] / agg["txn_count"]).round(4)
    agg["avg_ticket"] = (agg["gross_amount"] / agg["txn_count"]).round(2)

    # Ensure types for SQL
    agg["metric_date"] = pd.to_datetime(agg["metric_date"]).dt.date

    # Order columns to match table
    agg = agg[
        [
            "metric_date",
            "merchant_id",
            "txn_count",
            "approved_txn_count",
            "declined_txn_count",
            "gross_amount",
            "approved_amount",
            "approval_rate",
            "avg_ticket",
        ]
    ]

    return agg


# -----------------------------
# Load: Upsert merchants + facts
# -----------------------------
def load_merchants(engine, merchants: pd.DataFrame) -> int:
    # Load merchants using a staging table + MERGE
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('dbo.stg_merchants', 'U') IS NOT NULL DROP TABLE dbo.stg_merchants;"))
        conn.execute(
            text(
                """
                CREATE TABLE dbo.stg_merchants (
                    merchant_id   VARCHAR(64) NOT NULL,
                    merchant_name VARCHAR(255) NOT NULL,
                    category      VARCHAR(100) NULL,
                    city          VARCHAR(100) NULL,
                    state         VARCHAR(50)  NULL
                );
                """
            )
        )

    # Bulk insert to staging
    merchants_to_load = merchants.copy()
    for col in ["merchant_id", "merchant_name", "category", "city", "state"]:
        if col not in merchants_to_load.columns:
            merchants_to_load[col] = None

    merchants_to_load = merchants_to_load[["merchant_id", "merchant_name", "category", "city", "state"]]
    merchants_to_load.to_sql("stg_merchants", engine, schema="dbo", if_exists="append", index=False)

    merge_sql = """
    MERGE dbo.dim_merchants AS target
    USING dbo.stg_merchants AS src
    ON target.merchant_id = src.merchant_id
    WHEN MATCHED THEN
        UPDATE SET
            merchant_name = src.merchant_name,
            category = src.category,
            city = src.city,
            state = src.state
    WHEN NOT MATCHED THEN
        INSERT (merchant_id, merchant_name, category, city, state)
        VALUES (src.merchant_id, src.merchant_name, src.category, src.city, src.state);
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        # Optional cleanup
        conn.execute(text("DROP TABLE dbo.stg_merchants;"))

    return len(merchants_to_load)


def upsert_facts(engine, daily: pd.DataFrame) -> int:
    # Stage daily metrics + MERGE into fact table
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('dbo.stg_daily_metrics', 'U') IS NOT NULL DROP TABLE dbo.stg_daily_metrics;"))
        conn.execute(
            text(
                """
                CREATE TABLE dbo.stg_daily_metrics (
                    metric_date        DATE        NOT NULL,
                    merchant_id        VARCHAR(64) NOT NULL,
                    txn_count          INT         NOT NULL,
                    approved_txn_count INT         NOT NULL,
                    declined_txn_count INT         NOT NULL,
                    gross_amount       DECIMAL(18,2) NOT NULL,
                    approved_amount    DECIMAL(18,2) NOT NULL,
                    approval_rate      DECIMAL(9,4)  NOT NULL,
                    avg_ticket         DECIMAL(18,2) NOT NULL
                );
                """
            )
        )

    daily.to_sql("stg_daily_metrics", engine, schema="dbo", if_exists="append", index=False)

    merge_sql = """
    MERGE dbo.fact_daily_merchant_metrics AS target
    USING dbo.stg_daily_metrics AS src
    ON target.metric_date = src.metric_date AND target.merchant_id = src.merchant_id
    WHEN MATCHED THEN
        UPDATE SET
            txn_count = src.txn_count,
            approved_txn_count = src.approved_txn_count,
            declined_txn_count = src.declined_txn_count,
            gross_amount = src.gross_amount,
            approved_amount = src.approved_amount,
            approval_rate = src.approval_rate,
            avg_ticket = src.avg_ticket
    WHEN NOT MATCHED THEN
        INSERT (
            metric_date, merchant_id, txn_count, approved_txn_count, declined_txn_count,
            gross_amount, approved_amount, approval_rate, avg_ticket
        )
        VALUES (
            src.metric_date, src.merchant_id, src.txn_count, src.approved_txn_count, src.declined_txn_count,
            src.gross_amount, src.approved_amount, src.approval_rate, src.avg_ticket
        );
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text("DROP TABLE dbo.stg_daily_metrics;"))

    return len(daily)


def main():
    engine = get_engine()

    merchants, txns = extract()
    clean_txns, stats = validate_and_clean(merchants, txns)

    daily = transform_daily_metrics(clean_txns)

    stats.merchants_loaded = load_merchants(engine, merchants)
    stats.daily_rows = len(daily)
    stats.fact_rows_upserted = upsert_facts(engine, daily)

    print("\n===== ETL RUN SUMMARY =====")
    print(f"Transactions total:        {stats.txns_total}")
    print(f"After dedupe:              {stats.txns_after_dedupe}")
    print(f"Valid transactions:        {stats.txns_valid}")
    print(f"Rejected transactions:     {stats.txns_rejected}")
    print(f"Merchants loaded (upsert): {stats.merchants_loaded}")
    print(f"Daily metric rows:         {stats.daily_rows}")
    print(f"Fact rows upserted:        {stats.fact_rows_upserted}")
    print("==========================\n")
    print(" ETL completed successfully.")


if __name__ == "__main__":
    main()
