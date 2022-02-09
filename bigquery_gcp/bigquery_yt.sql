CREATE OR REPLACE EXTERNAL TABLE `sanjay-dtc-de.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_sanjay-dtc-de/raw/yellowtaxi/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_sanjay-dtc-de/raw/yellowtaxi/yellow_tripdata_2020-*.parquet']
);

-- Check yellow trip data
SELECT * FROM sanjay-dtc-de.trips_data_all.external_yellow_tripdata limit 10;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE sanjay-dtc-de.trips_data_all.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM sanjay-dtc-de.trips_data_all.external_yellow_tripdata;

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM sanjay-dtc-de.trips_data_all.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE sanjay-dtc-de.trips_data_all.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM sanjay-dtc-de.trips_data_all.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM sanjay-dtc-de.trips_data_all.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM sanjay-dtc-de.trips_data_all.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;