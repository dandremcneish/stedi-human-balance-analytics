"""
step_trainer_trusted.py

Glue Job: Landing -> Trusted (Step Trainer)

Populates step_trainer_trusted with the Step Trainer IoT records for
customers who are in customers_curated (i.e., who have accelerometer data
and have agreed to share their data for research).

IMPORTANT: The fulfillment website has a defect that reuses the same ~30
serial numbers across millions of customers, so serialNumber is NOT unique
in either table. Joining step_trainer_landing directly against
customers_curated on serialNumber would multiply every step_trainer row by
however many curated customers happen to share that serial number. To avoid
that while still using an INNER JOIN (rather than the IN-subquery semi-join
used previously), this job first builds a small distinct list of serial
numbers -- SELECT DISTINCT serialnumber FROM customers_curated -- and joins
step_trainer_landing against that deduplicated list instead of against
customers_curated directly. Joining to a distinct, one-row-per-serial-number
list gives the same duplicate-row protection as the semi-join while making
the join explicit.

IMPORTANT (idempotency): Glue's S3 sink APPENDS new files to the target
prefix on every run rather than overwriting it. Without an explicit purge,
rerunning this job (e.g. after a code fix) silently multiplies the row
count in step_trainer_trusted because old output from prior runs is never
removed. glueContext.purge_s3_path() is called immediately before the
write so each run replaces the previous output instead of stacking on top
of it, keeping step_trainer_trusted rerunnable and idempotent.

Reads : Glue Catalog tables stedi.step_trainer_landing, stedi.customers_curated
Writes: Glue Catalog table stedi.step_trainer_trusted
(s3://<bucket>/step_trainer/trusted/)
Expected row count after this job: 14460
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = "s3://stedi-dandre-mcneish-lake"
GLUE_DATABASE = "stedi"

step_trainer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="step_trainer_landing"
)
customers_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customers_curated"
)

step_trainer_landing_dyf.toDF().createOrReplaceTempView("step_trainer_landing")
customers_curated_dyf.toDF().createOrReplaceTempView("customers_curated")

step_trainer_trusted_df = spark.sql(
    """
    SELECT s.*
    FROM step_trainer_landing s
    INNER JOIN (
        SELECT DISTINCT serialnumber
        FROM customers_curated
    ) c
    ON s.serialNumber = c.serialnumber
    """
)

step_trainer_trusted_dyf = DynamicFrame.fromDF(
    step_trainer_trusted_df, glueContext, "step_trainer_trusted_dyf"
)

# Purge any previously written output at this path before writing the new
# result. Without this, each job run APPENDS new parquet files alongside the
# old ones at the same S3 prefix, so a rerun silently inflates the row count
# returned by Athena even though the transform logic itself is correct.
# Purging first makes the job idempotent/rerunnable.
glueContext.purge_s3_path(
    f"{S3_BUCKET}/step_trainer/trusted/", options={"retentionPeriod": 0}
)

sink = glueContext.getSink(
    connection_type="s3",
    path=f"{S3_BUCKET}/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="step_trainer_trusted"
)
sink.writeFrame(step_trainer_trusted_dyf)

job.commit()
