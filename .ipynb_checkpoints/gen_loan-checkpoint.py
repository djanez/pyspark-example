import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

N = 500  # cantidad de préstamos a generar

loan_types = ["personal", "hipotecario", "vehicular"]
statuses = ["ok", "late", "defaulter"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=int(np.random.randint(0, delta.days)))

rows = []

for loan_id in range(1, N + 1):

    customer_id = np.random.choice(range(1, int((N+1)*0.65)))
    capital = np.random.choice([1000, 2000, 3000, 5000, 8000, 10000, 15000, 25000])
    interest = round(capital * np.random.uniform(0.05, 0.15), 2)

    num_fees = int(np.random.choice([6, 10, 12, 18, 24, 36, 48]))
    total_amount = round(capital + interest, 2)
    fees = round(total_amount / num_fees, 2)

    num_payed_fees = np.random.randint(0, num_fees + 1)
    total_returned = round(fees * num_payed_fees, 2)

    begin_date = random_date(datetime(2018, 1, 1), datetime(2023, 12, 31))
    active = np.random.choice([True, False], p=[0.6, 0.4])

    end_date = None
    if not active:
        end_date = begin_date + timedelta(days=int(30 * num_fees))  # <-- corregido

    row = {
        "loan_id": loan_id,
        "customer_id": customer_id,
        "capital": float(capital),
        "interest": float(interest),
        "num_fees": num_fees,
        "fees": float(fees),
        "num_payed_fees": int(num_payed_fees),
        "total_returned": float(total_returned),
        "active": bool(active),
        "begin_date": begin_date.date(),
        "end_date": end_date.date() if end_date else None,
        "total_amount": float(total_amount),
        "loan_type": np.random.choice(loan_types),
        "status": np.random.choice(statuses, p=[0.7, 0.2, 0.1]),
        "warranty": bool(np.random.choice([True, False])),
        "remark": ""
    }

    rows.append(row)

df = pd.DataFrame(rows)

# --------------------------------------------------
# ERRORES INTENCIONADOS
# --------------------------------------------------

# 1️⃣ num_payed_fees > num_fees (3%)
idx = df.sample(frac=0.03).index
df.loc[idx, "num_payed_fees"] = df.loc[idx, "num_fees"] + np.random.randint(1, 5, size=len(idx))

# 2️⃣ active = TRUE pero end_date no es NULL (3%)
idx = df.sample(frac=0.03).index
df.loc[idx, "active"] = True
df.loc[idx, "end_date"] = df.loc[idx, "begin_date"] + pd.to_timedelta(100, unit="D")

# 3️⃣ active = FALSE pero end_date es NULL (2%)
idx = df.sample(frac=0.02).index
df.loc[idx, "active"] = False
df.loc[idx, "end_date"] = None

# 4️⃣ total_returned incorrecto (4%)
idx = df.sample(frac=0.04).index
df.loc[idx, "total_returned"] = df.loc[idx, "total_returned"] * np.random.uniform(0.3, 0.7, size=len(idx))

# 5️⃣ total_amount incorrecto (3%)
idx = df.sample(frac=0.03).index
df.loc[idx, "total_amount"] = df.loc[idx, "capital"] + np.random.randint(-1000, 1000, size=len(idx))

# 6️⃣ status incoherente con pagos (defaulter con todo pagado) (2%)
idx = df.sample(frac=0.02).index
df.loc[idx, "status"] = "defaulter"
df.loc[idx, "num_payed_fees"] = df.loc[idx, "num_fees"]

# 7️⃣ filas duplicadas (1%)
duplicates = df.sample(frac=0.01)
df = pd.concat([df, duplicates], ignore_index=True)

# --------------------------------------../data/-----UARDAR CSV
# --------------------------------------------------

df.to_csv("loans.csv", index=False)
print("CSV 'loans_with_errors.csv' generado correctamente.")