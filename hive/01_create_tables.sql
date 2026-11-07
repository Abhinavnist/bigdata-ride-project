CREATE DATABASE IF NOT EXISTS ride;
USE ride;

DROP TABLE IF EXISTS trips;
CREATE EXTERNAL TABLE trips (
  hvfhs_license_num STRING,
  dispatching_base_num STRING,
  request_datetime TIMESTAMP,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  PULocationID INT,
  DOLocationID INT,
  trip_miles DOUBLE,
  trip_time BIGINT,
  base_passenger_fare DOUBLE,
  tips DOUBLE,
  driver_pay DOUBLE
)
STORED AS PARQUET
LOCATION 'file:///data/raw/';

DROP TABLE IF EXISTS zones;
CREATE EXTERNAL TABLE zones (
  LocationID STRING,
  Borough STRING,
  Zone STRING,
  service_zone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 'file:///data/lookup/'
TBLPROPERTIES ("skip.header.line.count"="1");
