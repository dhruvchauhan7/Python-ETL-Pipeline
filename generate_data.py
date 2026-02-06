import random
from datetime import datetime, timedelta
import csv
import os

os.makedirs("data", exist_ok=True)

random.seed(42)

merchants = [
    ("m_1001", "Sunrise Coffee", "Cafe", "Costa Mesa", "CA"),
    ("m_1002", "Ocean Threads", "Retail", "Huntington Beach", "CA"),
    ("m_1003", "FitLab Gym", "Fitness", "Irvine", "CA"),
    ("m_1004", "ByteMart Electronics", "Electronics", "Anaheim", "CA"),
    ("m_1005", "Taco Town", "Restaurant", "Santa Ana", "CA"),
    ("m_1006", "Green Bowl", "Restaurant", "Tustin", "CA"),
    ("m_1007", "Peak Outdoors", "Retail", "Laguna Beach", "CA"),
]

# ------------------------
# Merchants CSV
# ------------------------
with open("data/merchants.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["merchant_id", "merchant_name", "category", "city", "state"])
    for m in merchants:
        writer.writerow(m)

# ------------------------
# Transactions CSV
# ------------------------
start_date = datetime(2026, 1, 1)
num_days = 30
txns_per_day = 250  # 30 * 250 = 7,500 transactions

statuses = ["APPROVED", "DECLINED"]
payment_methods = ["CARD", "WALLET"]

txn_rows = []
txn_id_counter = 1

for day in range(num_days):
    day_start = start_date + timedelta(days=day)

    for _ in range(txns_per_day):
        merchant = random.choice(merchants)
        ts = day_start + timedelta(
            minutes=random.randint(0, 1439),
            seconds=random.randint(0, 59),
        )

        amount = round(random.uniform(3.50, 250.00), 2)
        status = random.choices(statuses, weights=[0.85, 0.15])[0]
        method = random.choice(payment_methods)

        txn_rows.append([
            f"t_{txn_id_counter:06d}",
            merchant[0],
            ts.isoformat() + "Z",
            amount,
            status,
            method
        ])

        txn_id_counter += 1

# Inject a few bad records (realism)
txn_rows.append(["t_bad_1", "m_9999", "2026-01-10T10:00:00Z", 10.00, "APPROVED", "CARD"])
txn_rows.append(["t_bad_2", "m_1001", "2026-01-15T12:00:00Z", -5.00, "APPROVED", "CARD"])

with open("data/transactions.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "transaction_id",
        "merchant_id",
        "txn_ts_utc",
        "amount",
        "status",
        "payment_method",
    ])
    writer.writerows(txn_rows)

print("Generated data:")
print(f"Merchants: {len(merchants)}")
print(f"Transactions: {len(txn_rows)}")
