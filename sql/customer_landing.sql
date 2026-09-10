-- customer_landing.sql
-- Creates the customer_landing Glue table (Landing Zone) over the raw customer JSON
-- data from the STEDI fulfillment/website system.
-- Run this in the Athena query editor (Glue database must already exist, e.g. "stedi").
--
-- Bucket used for this run: stedi-dandre-mcneish-lake
-- Expected row count after creation: 956

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    serialnumber              STRING,
    sharewithpublicasofdate   BIGINT,
    birthday                  STRING,
    registrationdate          BIGINT,
    sharewithresearchasofdate BIGINT,
    customername              STRING,
    email                     STRING,
    lastupdatedate            BIGINT,
    phone                     STRING,
    sharewithfriendsasofdate  BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dandre-mcneish-lake/customer/landing/';
