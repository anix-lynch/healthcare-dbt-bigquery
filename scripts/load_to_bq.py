"""
Load healthcare CSV → BigQuery raw table.
Run once before dbt run.
"""
import os, sys, warnings
warnings.filterwarnings("ignore")

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

PROJECT_ID = "maps-platform-20251011-140544"
DATASET_ID = "healthcare_analytics"
TABLE_ID = "raw_patients"
KEYFILE = "/Users/anixlynch/.config/secrets/seo-service-account.json"
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "seeds", "healthcare_dataset.csv")

def main():
    creds = service_account.Credentials.from_service_account_file(KEYFILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)

    # Create dataset if needed
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} exists")
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
        print(f"Created dataset {DATASET_ID}")

    print(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["billing_amount"] = pd.to_numeric(df["billing_amount"], errors="coerce")
    df["date_of_admission"] = pd.to_datetime(df["date_of_admission"], errors="coerce").dt.date
    df["discharge_date"] = pd.to_datetime(df["discharge_date"], errors="coerce").dt.date
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded → {table_ref}")
    print(f"Row count: {client.get_table(table_ref).num_rows:,}")

if __name__ == "__main__":
    main()
