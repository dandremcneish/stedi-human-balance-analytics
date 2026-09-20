# STEDI Human Balance Analytics — Data Lakehouse (AWS Glue / Athena / S3)

STEDI is a balance-training device company. Their Step Trainer hardware and companion mobile app generate three raw data streams: customer profile data, mobile accelerometer readings, and Step Trainer sensor readings. This project builds a landing → trusted → curated lakehouse on AWS Glue, Athena, and S3 that turns those three streams into a single privacy-filtered, ML-ready dataset for training a step-detection model, dropping any customer who hasn't consented to share their data before it reaches anything else.

Five Glue ETL jobs move the data through progressively more trustworthy layers, with every stage verified against real row counts pulled live from Athena rather than assumed to have worked.

## Architecture

**Landing** (raw, as-ingested) → **Trusted** (privacy-filtered, deduplicated) → **Curated** (joined, ML-ready)

| Stage | Job | Output | Verified rows |
|---|---|---|---|
| Landing | `sql/customer_landing.sql` | `customer_landing` | 956 |
| Landing | `sql/accelerometer_landing.sql` | `accelerometer_landing` | 81,273 |
| Landing | `sql/step_trainer_landing.sql` | `step_trainer_landing` | 28,680 |
| Trusted | `glue_jobs/customer_landing_to_trusted.py` | `customer_trusted` | 482 |
| Trusted | `glue_jobs/accelerometer_landing_to_trusted.py` | `accelerometer_trusted` | 40,981 |
| Curated | `glue_jobs/customer_trusted_to_curated.py` | `customers_curated` | 482 |
| Curated | `glue_jobs/step_trainer_trusted.py` | `step_trainer_trusted` | 14,460 |
| Curated | `glue_jobs/machine_learning_curated.py` | `machine_learning_curated` | 43,681 |

Screenshots of every count, pulled live from the AWS console, are in `screenshots/`.

## Why the numbers drop at each stage

The trusted layer is where consent filtering happens: `customer_landing` starts at 956 rows, but only 482 customers had actually agreed to share data for research, so `customer_trusted` and everything downstream is built from that smaller, consented population. `accelerometer_trusted` (40,981 rows) is the accelerometer stream joined against consented customers only. The final `machine_learning_curated` table (43,681 rows) joins curated customers, trusted accelerometer readings, and step trainer readings into the single table the step-detection model actually trains on.

## Repo layout

```
stedi/
├── README.md
├── sql/
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   └── step_trainer_landing.sql
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── step_trainer_trusted.py
│   └── machine_learning_curated.py
├── stand_out/
│   ├── accelerometer_landing_to_trusted_standout.py
│   └── machine_learning_curated_standout.py
└── screenshots/
    ├── 01_customer_landing_count_956.jpg
    ├── 02_accelerometer_landing_count_81273.jpg
    ├── 03_step_trainer_landing_count_28680.jpg
    ├── 04_customer_landing_blank_research_date.jpg
    ├── 05_customer_trusted_count_482.jpg
    ├── 06_accelerometer_trusted_count_40981.jpg
    ├── 07_customers_curated_count_482.jpg
    ├── 08_step_trainer_trusted_count_14460.jpg
    └── 09_machine_learning_curated_count_43681.jpg
```

## Running it

1. Provision an AWS environment (S3, Glue, Athena) in `us-east-1` and create an S3 bucket for the lake.
2. Load the three source datasets (customer profiles, accelerometer readings, step trainer readings) into `landing/` prefixes in that bucket.
3. Create an Athena database called `stedi`.
4. Run each file in `sql/` in the Athena query editor to build the three landing tables.
5. Run the Glue jobs in order: the two trusted-zone jobs, then the three curated-zone jobs.

All SQL/Python in this repo references the bucket used for this run (`stedi-dandre-mcneish-lake`); swap in your own bucket name if you're running it fresh.

## A stricter variant

`stand_out/` holds two alternate versions of the trusted/curated jobs that go further: dropping any accelerometer reading recorded before the customer's consent date, and stripping email/user identifiers from the final ML table so it's fully anonymized. Row counts under that stricter version: `accelerometer_trusted` 32,025, `customers_curated` 464, `step_trainer_trusted` 14,460 (unchanged), `machine_learning_curated` 34,437. Not used for the run documented above, included for reference.

## Notes from building this

- **Access Denied running a Glue job** almost always traces back to the Glue IAM role's permissions or trust relationship for S3 access, not the job code itself.
- **`step_trainer_trusted` returning 0 rows** is why that job filters with `WHERE serialNumber IN (...)` instead of an `INNER JOIN`: serial numbers aren't unique in the source data, and Spark's join behaves differently from Presto/Athena on non-unique join keys, silently dropping rows a plain join would keep.
- Re-running a job after edits requires deleting the old Athena table and old S3 output files first, or you'll see stale or duplicated data sitting alongside the new run.
