CREATE OR REPLACE EXTERNAL TABLE `sanjay-dtc-de.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_sanjay-dtc-de/raw/fhv/fhv_tripdata_2019-*.parquet']
);

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE sanjay-dtc-de.trips_data_all.fhv_tripdata_partitioned_clustered
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM sanjay-dtc-de.trips_data_all.external_fhv_tripdata;

--1.What is count for fhv vehicles data for year 2019
SELECT count(*) FROM sanjay-dtc-de.trips_data_all.external_fhv_tripdata

--2.How many distinct dispatching_base_num we have in fhv for 2019
SELECT count(distinct(dispatching_base_num)) FROM sanjay-dtc-de.trips_data_all.external_fhv_tripdata

--4.What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
SELECT count(*) as trips
FROM `sanjay-dtc-de.trips_data_all.fhv_tripdata_partitoned_clustered`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');