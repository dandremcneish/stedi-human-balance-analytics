-- accelerometer_landing.sql
-- Creates the accelerometer_landing Glue table (Landing Zone) over the raw
-- accelerometer JSON data from the STEDI mobile app.
-- Run this in the Athena query editor.
--
-- Bucket used for this run: stedi-dandre-mcneish-lake
-- Expected row count after creation: 81273

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    user      STRING,
    timestamp BIGINT,
    x         DOUBLE,
    y         DOUBLE,
    z         DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dandre-mcneish-lake/accelerometer/landing/';
