-- step_trainer_landing.sql
-- Creates the step_trainer_landing Glue table (Landing Zone) over the raw
-- Step Trainer motion-sensor JSON data (IoT stream).
-- Run this in the Athena query editor.
--
-- Bucket used for this run: stedi-dandre-mcneish-lake
-- Expected row count after creation: 28680

CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorReadingTime   BIGINT,
    serialNumber        STRING,
    distanceFromObject  INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dandre-mcneish-lake/step_trainer/landing/';
