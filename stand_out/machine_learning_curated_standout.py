"""
machine_learning_curated_standout.py  (OPTIONAL - stand-out version)

Same as machine_learning_curated.py, but drops the customer's email/user
identifier (and any other directly-identifying field) from the final
curated table. This anonymizes the ML training data so it is no longer
personal data subject to GDPR/CCPA-style deletion requests -- if a customer
later revokes consent or asks for deletion, this table is unaffected
because it no longer contains anything that identifies them.

Use this INSTEAD OF machine_learning_curated.py if you're following the
stand-out suggestions (expected row count: 34437, once combined with the
stand-out accelerometer_trusted table).
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

step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="step_trainer_trusted"
)
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="accelerometer_trusted"
)

step_trainer_trusted_dyf.toDF().createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_dyf.toDF().createOrReplaceTempView("accelerometer_trusted")

# --- Transform: join on timestamp, but DO NOT select a.user (email) or any
#     other identifying field -- only the sensor measurements survive ---
machine_learning_curated_df = spark.sql(
    """
    SELECT
        s.sensorReadingTime,
        s.serialNumber,
        s.distanceFromObject,
        a.timestamp,
        a.x,
        a.y,
        a.z
    FROM step_trainer_trusted s
    JOIN accelerometer_trusted a
      ON s.sensorReadingTime = a.timestamp
    """
)

machine_learning_curated_dyf = DynamicFrame.fromDF(
    machine_learning_curated_df, glueContext, "machine_learning_curated_dyf"
)

sink = glueContext.getSink(
    connection_type="s3",
    path=f"{S3_BUCKET}/machine_learning/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE, catalogTableName="machine_learning_curated"
)
sink.writeFrame(machine_learning_curated_dyf)

job.commit()
