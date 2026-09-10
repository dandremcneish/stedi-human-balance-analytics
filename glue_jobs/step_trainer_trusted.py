"""
step_trainer_trusted.py

Glue Job: Landing -> Trusted (Step Trainer)

Populates step_trainer_trusted with the Step Trainer IoT records for
customers who are in customers_curated (i.e., who have accelerometer data
and have agreed to share their data for research).

IMPORTANT: The fulfillment website has a defect that reuses the same ~30
serial numbers across millions of customers, so serialnumber is NOT unique
in either table. An INNER JOIN on serialNumber will silently multiply rows
(or, inside Glue's Spark engine specifically, can return zero rows -- see
the project's Troubleshooting page). The correct approach is a semi-join:
keep every step_trainer_landing row whose serialNumber also appears
somewhere in customers_curated, without actually joining/duplicating rows.
This is written as a "WHERE ... IN (...)" SQL query rather than a visual
Join node, per the project's troubleshooting guidance.

Reads    : Glue Catalog tables  stedi.step_trainer_landing, stedi.customers_curated
Writes   : Glue Catalog table   stedi.step_trainer_trusted
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

# ---- CONFIG: replace with your own S3 bucket / Glue database name ----
S3_BUCKET = "s3://stedi-dandre-mcneish-lake"
GLUE_DATABASE = "stedi"

# --- Read sources from the Data Catalog ---
step_trainer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="step_trainer_landing"
)
customers_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customers_curated"
)

step_trainer_landing_dyf.toDF().createOrReplaceTempView("step_trainer_landing")
customers_curated_dyf.toDF().createOrReplaceTempView("customers_curated")

# --- Transform: semi-join on serialNumber (deliberately NOT an inner join --
#     see module docstring above for why) ---
step_trainer_trusted_df = spark.sql(
    """
    SELECT s.*
    FROM step_trainer_landing s
    WHERE s.serialNumber IN (
        SELECT serialnumber FROM customers_curated
    )
    """
)

step_trainer_trusted_dyf = DynamicFrame.fromDF(
    step_trainer_trusted_df, glueContext, "step_trainer_trusted_dyf"
)

# --- Write result to S3 + update the Glue Data Catalog (Trusted Zone) ---
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
