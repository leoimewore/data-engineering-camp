CREATE EXTERNAL TABLE `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://dataengineerproject-*1/yellow_tripdata_2024-*.parquet']
    );

SELECT COUNT(*) FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external`

SELECT COUNT(DISTINCT PULocationID) FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external`;

SELECT COUNT(DISTINCT PULocationID) FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_tripdata_parquet`

SELECT PULocationID FROM  `dataengineerproject-448203.zoomcamp_dataset.yellow_tripdata_parquet` ;

SELECT PULocationID,DOLocationID FROM  `dataengineerproject-448203.zoomcamp_dataset.yellow_tripdata_parquet` ;

SELECT COUNT(*) FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external`
WHERE
fare_amount = 0

CREATE OR REPLACE TABLE `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external`;

SELECT DISTINCT PULocationID FROM dataengineerproject-448203.zoomcamp_dataset.yellow_tripdata_parquet
WHERE 
DATE(tpep_dropoff_datetime) BETWEEN "2022-03-01" AND "2022-03-15"

DROP TABLE `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external_partitioned`;


SELECT DISTINCT PULocationID FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_taxi_2024_external_partitioned_clustered`

WHERE 
DATE(tpep_dropoff_datetime) BETWEEN "2022-03-01" AND "2022-03-15"


SELECT COUNT(*) FROM `dataengineerproject-448203.zoomcamp_dataset.yellow_tripdata_parquet`






