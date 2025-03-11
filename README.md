# Data warehousing + ETL

## Introduction
In this data engineering project we present a complete ETL pipeline deployed entirely with Docker containers.
We simulate a typical data flow from raw data ingestion to analytics and visualization.

We start with a CSV dataset obtained from Kaggle, which is ingested into MongoDB, 
where Mongo acts as a data lake in this architecture. 
From this point we perform an ETL process using Apache Spark to clean, 
transform, normalize the data and then loading it into a PostgreSQL data warehouse.

Data loaded into PostgreSQL is formated into a Snowflake schema,
breaking down the original data structure into multiple relational tables and intermediate tables that support efficient querying and analysis.


## Current workflow
- **Data source:** A Kaggle CSV file containing data from thousands of shows on Netflix (see [Netflix Movies and TV Shows(2008-2021)](https://www.kaggle.com/datasets/shivamb/netflix-shows)).
- **Data lake**: Raw data is imported into MongoDB, which plays the role of a centralized storage for raw and semi-processed data.
- **ETL processing**: Spark reads stored data in MongoDB, performs a series of data cleaning and formatting steps and exports the cleaned and normalized data into PostgreSQL.
- **Data warehouse**: Processed data is stored following a Snowflake schema, which includes fact tables (shows), dimension tables, and relational (intermediate) tables.

## Upcoming Features
- **SQL analytics**: Run a series of complex SQL queries in PostgreSQL to answer specific business questions.
- **Visualization**: Build an interactive dashboard using Looker Studio to present key insights and metrics.

## Technology Stack
- **Docker**: The entire data processing pipeline runs using containerized services for MongoDB, PostgreSQL, Spark (including Python environment) and pgAdmin.
- **MongoDB**: Works as a data lake layer for storing raw data.
- **Apache Spark**: Handles the data processing and transformation portion of this project.
- **PostgreSQL**: Stores normalized, structured data ready for analytics.
- **Looker Studio** (coming soon): For data visualization and reporting.

## Requirements 
- **Docker**: to run this project you require a functional copy Docker. And docker CLI commands from terminal.

## How to run
1. Clone this repository to your local system.
2. To setup the docker services, open a terminal inside the repository and run the following docker command: 
   ```shell 
   docker-compose up -d 
   ```
3. To run the pipeline run the following commands:
   1. For data ingestion
         ```shell
        docker exec pyspark_container python3 ./ETL/data_ingestion.py
         ```
   2. For running the ETL process
      ```shell 
        docker exec pyspark_container python3 ./ETL/ETL.py 
      ```
4. You can access the process data via pgAdmin directly from your browser at:
   ```html
   http://localhost:5050/
   ```


