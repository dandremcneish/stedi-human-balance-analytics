"""
machine_learning_curated.py

Glue Job: Trusted -> Curated (Machine Learning training table)

Creates the final machine_learning_curated table: each Step Trainer reading
paired with the accelerometer reading recorded at the same timestamp, for
customers who agreed to share their data for research. This is the table
the STEDI Data Science team will use to train the step-detection model.

Reads    : Glue Catalog tables  stedi.step_trainer_trusted, stedi.accelerometer_trusted
Writes   : Glue Catalog table   stedi.machine_learning_curated
            (s3://<bucket>/machine_learning/curated/)
Rubric   : "Write a Glue Job to create curated data" -- inner joins
           step_trainer_trusted with accelerometer_trusted by sensor
           reading time / timestamp.
Expected row count after this job: 43681
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
step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="step_trainer_trusted"
)
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="accelerometer_trusted"
)

step_trainer_trusted_dyf.toDF().createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_dyf.toDF().createOrReplaceTempView("accelerometer_trusted")

# --- Transform: join Step Trainer readings to accelerometer readings by
#     matching timestamp ---
machine_learning_curated_df = spark.sql(
    """
    SELECT
        s.sensorReadingTime,
        s.serialNumber,
        s.distanceFromObject,
        a.user,
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

# --- Write result to S3 + update the Glue Data Catalog (Curated Zone) ---
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
