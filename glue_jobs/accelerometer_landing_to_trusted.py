"""
accelerometer_landing_to_trusted.py

Glue Job: Landing -> Trusted (Accelerometer)

Sanitizes the Accelerometer data from the STEDI mobile app (Landing Zone) and
keeps only the Accelerometer Readings belonging to customers who agreed to
share their data for research purposes (Trusted Zone).

Reads    : Glue Catalog tables  stedi.accelerometer_landing, stedi.customer_trusted
Writes   : Glue Catalog table   stedi.accelerometer_trusted
            (s3://<bucket>/accelerometer/trusted/)
Rubric   : "Join Privacy tables with Glue Jobs" -- inner joins customer_trusted
           with accelerometer_landing by email; output has only the
           accelerometer columns.
Expected row count after this job: 40981
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
accelerometer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="accelerometer_landing"
)
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customer_trusted"
)

accelerometer_landing_dyf.toDF().createOrReplaceTempView("accelerometer_landing")
customer_trusted_dyf.toDF().createOrReplaceTempView("customer_trusted")

# --- Transform: inner join by email, keep ONLY the accelerometer columns ---
accelerometer_trusted_df = spark.sql(
    """
    SELECT a.user, a.timestamp, a.x, a.y, a.z
    FROM accelerometer_landing a
    JOIN customer_trusted c
      ON a.user = c.email
    """
)

accelerometer_trusted_dyf = DynamicFrame.fromDF(
    accelerometer_trusted_df, glueContext, "accelerometer_trusted_dyf"
)

# --- Write result to S3 + update the Glue Data Catalog (Trusted Zone) ---
sink = glueContext.getSink(
    connection_type="s3",
    path=f"{S3_BUCKET}/accelerometer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="accelerometer_trusted"
)
sink.writeFrame(accelerometer_trusted_dyf)

job.commit()
