Data Pipeline: Kafka → Spark → AWS S3 → Snowflake
-------------------------------------------------
Overview
--------
This project implements a real-time data pipeline that ingests, processes, and stores streaming data using Python, Apache Kafka, Apache Spark, AWS S3, and Snowflake.

Architecture Flow :-

1. Data Ingestion (Kafka)

- Apache Kafka is used as the messaging backbone.

- Producers send streaming data to Kafka topics.

- Kafka ensures durability and high throughput for incoming data.

2. Stream Processing (Apache Spark Structured Streaming)

- Spark consumes messages from Kafka in real time.

- Data is cleaned, transformed, and enriched using PySpark code.

- Schema handling and aggregations are performed at this stage.

3. Storage in AWS S3

- Processed data is written to AWS S3 in CSV format.

- S3 serves as a data lake for both raw and transformed datasets.

4. Data Warehousing in Snowflake

- AWS S3 data is ingested into Snowflake via Snowpipe .

- Snowflake enables fast SQL queries, BI dashboards, and analytics.
___________________________________________________________________
Tech Stack
----------
- Python (VS Code)	Development & scripting
- Apache Kafka	Real-time data ingestion
- Apache Spark	Stream processing & transformations
- AWS S3	Cloud storage 
- Snowflake	Cloud data warehouse
____________________________________________________________________
Folder Structure
--------------------
- kafka_producer         # Python scripts to push data to Kafka
- spark_streaming        # PySpark code for Kafka → S3 processing
- snowflake_integration  # Scripts & SQL for Snowflake ingestion
- README.md              # Project documentation
______________________________________________________________________

How It Works
-------------

- Start Kafka and create required topics.

- Run the producer script to send data to Kafka.

- Launch Spark streaming job to consume from Kafka and write to AWS S3.

- Trigger Snowflake ingestion via Snowpipe .

- Query data in Snowflake using SQL.
