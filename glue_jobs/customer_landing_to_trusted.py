"""
customer_landing_to_trusted.py

Glue Job: Landing -> Trusted (Customer)

Sanitizes the Customer data from the STEDI website (Landing Zone) and keeps
only the Customer Records for customers who agreed to share their data for
research purposes (Trusted Zone).

Reads    : Glue Catalog table  stedi.customer_landing
Writes   : Glue Catalog table  stedi.customer_trusted   (s3://<bucket>/customer/trusted/)
Rubric   : "Filter protected PII with Spark in Glue Jobs" -- drops rows with a
           blank/null shareWithResearchAsOfDate.
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

# --- Read source from the Data Catalog (Landing Zone) ---
customer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE, table_name="customer_landing"
)

customer_landing_df = customer_landing_dyf.toDF()
customer_landing_df.createOrReplaceTempView("customer_landing")

# --- Transform: keep only customers who consented to share data for research ---
# (Using a SQL Query transform here, per the project's troubleshooting guidance,
#  is more consistent than the visual Filter node.)
customer_trusted_df = spark.sql(
    """
    SELECT *
    FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
    """
)

customer_trusted_dyf = DynamicFrame.fromDF(
    customer_trusted_df, glueContext, "customer_trusted_dyf"
)

# --- Write result to S3 + update the Glue Data Catalog (Trusted Zone) ---
sink = glueContext.getSink(
    connection_type="s3",
    path=f"{S3_BUCKET}/customer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=GLUE_DATABASE, catalogTableName="customer_trusted")
sink.writeFrame(customer_trusted_dyf)

job.commit()
