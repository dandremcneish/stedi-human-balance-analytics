"""
accelerometer_landing_to_trusted_standout.py  (OPTIONAL - stand-out version)

Same as accelerometer_landing_to_trusted.py, but additionally filters out any
accelerometer reading recorded BEFORE the customer's research-consent date.
This protects against the case where a customer later revokes consent: we
can prove that every reading actually used for research was collected while
consent was already in place.

Use this INSTEAD OF accelerometer_landing_to_trusted.py if you want the
stand-out row counts:
    accelerometer_trusted: 32025 (instead of 40981)
    customers_curated:     464   (instead of 482)   [some customers lose all
                                                      their post-consent readings]
    step_trainer_trusted:  14460 (unchanged)
    machine_learning_curated: 34437 (instead of 43681)
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

accelerometer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="accelerometer_landing"
)
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customer_trusted"
)

accelerometer_landing_dyf.toDF().createOrReplaceTempView("accelerometer_landing")
customer_trusted_dyf.toDF().createOrReplaceTempView("customer_trusted")

# --- Transform: inner join by email AND only keep readings recorded on/after
#     the customer's consent date ---
accelerometer_trusted_df = spark.sql(
    """
    SELECT a.user, a.timestamp, a.x, a.y, a.z
    FROM accelerometer_landing a
    JOIN customer_trusted c
      ON a.user = c.email
     AND a.timestamp >= c.sharewithresearchasofdate
    """
)

accelerometer_trusted_dyf = DynamicFrame.fromDF(
    accelerometer_trusted_df, glueContext, "accelerometer_trusted_dyf"
)

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
