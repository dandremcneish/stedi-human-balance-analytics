# STEDI Human Balance Analytics — Data Lakehouse (AWS Glue / Athena / S3)

This is the D609 "Spark and Data Lakes" project: build a lakehouse (Landing →
Trusted → Curated) for the STEDI Step Trainer sensor data so the Data Science
team can train a step-detection ML model, while only ever using data from
customers who consented to research use.

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

## 0. One-time setup

1. Launch your AWS Cloud Lab, open the AWS Console, confirm you're in `us-east-1`.
2. Create an S3 bucket (any globally-unique name, e.g. `stedi-<yourname>-lake`).
3. Copy the source landing data from the course bucket into your own bucket:

```bash
aws s3 cp --recursive s3://cd0030bucket/customers/      s3://stedi-dandre-mcneish-lake/customer/landing/
aws s3 cp --recursive s3://cd0030bucket/accelerometer/  s3://stedi-dandre-mcneish-lake/accelerometer/landing/
aws s3 cp --recursive s3://cd0030bucket/step_trainer/   s3://stedi-dandre-mcneish-lake/step_trainer/landing/
```

4. In Athena / Glue, create a database called `stedi` (Athena: `CREATE DATABASE stedi;`).
5. All SQL/Python files in this repo already use the actual bucket
   (`stedi-dandre-mcneish-lake`) that this run used.

## 1. Landing Zone — create the 3 tables (Athena)

Run each file in `sql/` in the Athena query editor, in any order:

- `customer_landing.sql` → **956 rows** (verified, see screenshots/01)
- `accelerometer_landing.sql` → **81273 rows** (verified, see screenshots/02)
- `step_trainer_landing.sql` → **28680 rows** (verified, see screenshots/03)

Screenshot 04 shows `customer_landing` rows with a blank `sharewithresearchasofdate`.

## 2. Trusted Zone — 2 Glue jobs, in this order

1. `customer_landing_to_trusted.py` → `customer_trusted` = **482 rows** (screenshots/05)
2. `accelerometer_landing_to_trusted.py` → `accelerometer_trusted` = **40981 rows** (screenshots/06)

## 3. Curated Zone — 3 more Glue jobs, in this order

1. `customer_trusted_to_curated.py` → `customers_curated` = **482 rows** (screenshots/07)
2. `step_trainer_trusted.py` → `step_trainer_trusted` = **14460 rows**
3. `machine_learning_curated.py` → `machine_learning_curated` = **43681 rows** (screenshots/09)

(`step_trainer_trusted` count of 14460 was verified live in Athena; screenshot 08.)

## 4. Row-count checklist — all verified against the rubric

| Zone     | Table                     | Expected rows | Actual (verified) |
|----------|---------------------------|---------------|--------------------|
| Landing  | customer_landing          | 956           | 956 |
| Landing  | accelerometer_landing     | 81273         | 81273 |
| Landing  | step_trainer_landing      | 28680         | 28680 |
| Trusted  | customer_trusted          | 482           | 482 |
| Trusted  | accelerometer_trusted     | 40981         | 40981 |
| Trusted  | step_trainer_trusted      | 14460         | 14460 |
| Curated  | customers_curated         | 482           | 482 |
| Curated  | machine_learning_curated  | 43681         | 43681 |

## 5. Optional stand-out version

Swap in the two scripts in `stand_out/` instead of their base equivalents
(`accelerometer_landing_to_trusted.py` and `machine_learning_curated.py`) if
you want to also: (a) drop any accelerometer reading recorded before the
customer's consent date, and (b) strip email/user identifiers from the final
ML table so it's fully anonymized. Expected row counts then become:
accelerometer_trusted 32025, customers_curated 464, step_trainer_trusted
14460 (unchanged), machine_learning_curated 34437. (Not used for this run —
included for reference only.)

## 6. Troubleshooting reminders (from the course)

- **Access Denied running a Glue Job** → check the Glue IAM role's permissions
  and trust relationship for S3 access.
- **step_trainer_trusted returns 0 rows** → this is why `step_trainer_trusted.py`
  uses `WHERE serialNumber IN (...)` instead of an `INNER JOIN` — serial
  numbers are not unique, and Spark's join behaves differently from Presto/
  Athena on non-unique join keys.
- Whenever you edit and re-run a job: make sure the job is **saved** first,
  then delete the old Athena table and old S3 output files before re-running,
  or you'll see stale/duplicated data.
