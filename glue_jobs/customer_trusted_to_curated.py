"""
customer_trusted_to_curated.py

Glue Job: Trusted -> Curated (Customer)

Creates the customers_curated Glue Table: only customers who (a) agreed to
share data for research AND (b) actually have accelerometer readings on file.
This resolves the serial-number bug described in the project instructions --
customers_curated is later joined to the Step Trainer data by serial number.

Reads    : Glue Catalog tables  stedi.customer_trusted, stedi.accelerometer_trusted
Writes   : Glue Catalog table   stedi.customers_curated
            (s3://<bucket>/customer/curated/)
Rubric   : "Write a Glue Job to join trusted data" -- inner joins customer_trusted
           with accelerometer_trusted by email; output has only the customer
           columns.
Expected row count after this job: 482
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
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customer_trusted"
)
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="accelerometer_trusted"
)

customer_trusted_dyf.toDF().createOrReplaceTempView("customer_trusted")
accelerometer_trusted_dyf.toDF().createOrReplaceTempView("accelerometer_trusted")

# --- Transform: inner join by email, keep ONLY the customer columns, dedup ---
customers_curated_df = spark.sql(
    """
    SELECT DISTINCT c.*
    FROM customer_trusted c
    JOIN accelerometer_trusted a
      ON c.email = a.user
    """
)

customers_curated_dyf = DynamicFrame.fromDF(
    customers_curated_df, glueContext, "customers_curated_dyf"
)

# --- Write result to S3 + update the Glue Data Catalog (Curated Zone) ---
sink = glueContext.getSink(
    connection_type="s3",
    path=f"{S3_BUCKET}/customer/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName="customers_curated")
sink.writeFrame(customers_curated_dyf)

job.commit()
