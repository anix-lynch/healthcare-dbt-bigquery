# Healthcare Analytics — dbt + BigQuery

**Clinical data warehouse built on BigQuery with dbt.**
55,500 patient records → staging layer → 3 analytics marts.

**Stack:** Python · pandas · Google BigQuery · dbt · SQL

---

## Architecture

```
healthcare_dataset.csv (55,500 rows)
    ↓
scripts/load_to_bq.py          ← pandas → BigQuery raw table
    ↓
models/staging/stg_patients    ← clean, type-cast, derive LOS
    ↓
models/marts/
  ├── mart_condition_kpis       ← KPIs per medical condition
  ├── mart_billing_by_insurer   ← revenue cycle by payer
  └── mart_monthly_admissions   ← time-series trends + cumulative
```

---

## dbt Models

### Staging

**`stg_patients`** — cleans raw records, derives computed fields:
- `length_of_stay_days` = `discharge_date` − `date_of_admission`
- Filters: removes rows with null dates or zero billing
- Standardizes column names (snake_case)

### Marts

**`mart_condition_kpis`** — per-condition clinical benchmarks:

| Column | Description |
|--------|-------------|
| `total_patients` | Patient volume |
| `avg_billing_usd` | Mean claim value |
| `avg_los_days` | Average length of stay |
| `emergency_pct` | % Emergency admissions |
| `abnormal_test_pct` | % Abnormal test results |
| `unique_doctors` | Provider diversity |

**`mart_billing_by_insurer`** — payer mix analysis:
- Total, avg, min, max, median claim per insurer
- Avg LOS and abnormal rate by payer

**`mart_monthly_admissions`** — time-series (61 months):
- Admissions by type × month
- Running cumulative admissions
- Revenue + LOS trends over time

---

## Data Schema

Input: `healthcare_dataset.csv`

| Column | Type | Notes |
|--------|------|-------|
| Name | STRING | Patient (synthetic) |
| Age | INT | |
| Medical Condition | STRING | Cancer, Obesity, Diabetes, Hypertension, Asthma, Arthritis |
| Date of Admission | DATE | |
| Discharge Date | DATE | |
| Admission Type | STRING | Emergency / Urgent / Elective |
| Insurance Provider | STRING | Aetna, Blue Cross, Cigna, Medicare, UnitedHealthcare |
| Billing Amount | FLOAT | USD |
| Test Results | STRING | Normal / Inconclusive / Abnormal |

---

## Setup

### 1. GCP Project + BigQuery

```bash
# Create a service account with BigQuery Admin role
# Download key JSON → set env var
export GCP_PROJECT_ID=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Or use OAuth (ADC):
```bash
gcloud auth application-default login
export GCP_PROJECT_ID=your-project-id
```

### 2. Install dependencies

```bash
pip install dbt-bigquery pandas google-cloud-bigquery db-dtypes
```

### 3. Load raw data to BigQuery

```bash
python3 scripts/load_to_bq.py
# → creates dataset healthcare_analytics
# → loads 55,500 rows into raw_patients table
```

### 4. Copy and configure dbt profile

```bash
cp profiles.yml.example profiles.yml
# Edit: set project, keyfile path
```

### 5. Run dbt

```bash
dbt debug                  # verify connection
dbt run                    # build all models
dbt test                   # run data quality tests
dbt docs generate          # generate lineage docs
dbt docs serve             # open in browser
```

---

## dbt Tests

| Test | Model | What it checks |
|------|-------|----------------|
| `not_null` | `stg_patients.medical_condition` | No null conditions |
| `not_null` | `stg_patients.billing_amount` | No null billing |
| `accepted_values` | `raw_patients.admission_type` | Only Emergency/Urgent/Elective |
| `unique` | `mart_condition_kpis.medical_condition` | One row per condition |
| `unique` | `mart_billing_by_insurer.insurance_provider` | One row per insurer |

---

## Why This Stack

Healthcare ops teams need structured, tested data — not ad-hoc queries. This pattern:

- **BigQuery** — serverless, scales to millions of records, $0 for <1TB queries/month
- **dbt staging layer** — single source of clean truth, reusable across all marts
- **dbt marts** — domain-specific aggregates optimized for BI tools (Looker, Data Studio, Tableau)
- **dbt tests** — data quality gates prevent bad data reaching dashboards
- **dbt docs** — auto-generated lineage for compliance and audit

---

## Adapt for Your Use Case

- **Swap the source** — replace CSV load with Cloud Healthcare API, HL7/FHIR feed, or EHR export
- **Add FHIR layer** — map Patient/Encounter/Observation resources to staging schema
- **Connect BI tool** — point Looker Studio or Tableau to `healthcare_analytics.mart_*` tables
- **Schedule** — wrap in Cloud Composer (Airflow) or Cloud Scheduler for daily refresh

---

**Built by Anix Lynch** — ex-VC GP, ex-BlackRock RE, founder of [gozeroshot.dev](https://gozeroshot.dev).
Available on Upwork for healthcare data engineering, dbt modeling, and BigQuery pipeline work.
