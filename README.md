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
- **Staging Zone**: Raw data is imported into MongoDB. This is a temporary storing solution with a flexible data schema prior to data transformation.
- **Data transformation**: Spark reads stored data in MongoDB, performs a series of data cleaning and formatting steps and exports the cleaned and normalized data by applying a snowflake data schema prior to export it into PostgreSQL.
- **Data storing**: Processed data is stored following a Snowflake schema, which includes fact tables (shows), dimension tables, and relational (intermediate) tables. We add table relationships and constrains, and perform a series of SQL queries data are stored as Materialized views. Additionally, we export data as CSV files that will be used in the _Data visualization_ section. We can handle and monitor data stored via pgAdmin if desired.
- **Data visualization**: Exported CSV files are use by _Looker Studio_ to generate insightful data visualizations.

The basic architecture of this project is shown below:

![architecture_schema](/doc/figures/architecture_schemadrawio.png)

For obtaining more information about the architecture please refer to [Architecture](./doc/architecture.md)

## Upcoming Features
- ETL code optimization
- Looker Studio visualizations

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
2. To set up the docker services, open a terminal inside the repository and run the following docker command: 
   ```shell 
   docker-compose up -d 
   ```

3. To run the pipeline run the following commands:
   - **Data ingestion**. This command will download the dataset from Kaggle and store it into a MongoDB database without any data alterations:
     ```shell
         docker exec pyspark_container python3 ./ETL/data_ingestion.py
     ```
   - **ETL process**. This script gathers the data from Mongo (_extracts_) and apply a series of basic data cleaning tasks (i.e. remove duplicates, null values). It also formats and splits data into multiple tables following a Snowflake data schema.
      ```shell 
        docker exec pyspark_container python3 ./ETL/ETL.py 
      ```
You can find more information about the ETL process at [ETL_process](/doc/ETL_process.md).

4. You can access the processed data via pgAdmin directly from your browser at:
   ```html
   http://localhost:5050/
   ```
You will be prompted to insert the pgadmin password (which is `admin`:`adminpassword` ) and 
then again when accessing the database (`user`:`password`).
Notice that at this point no table relationship or any 
constrains has been properly set up in the database. 
You can manually set these relationships via pgAdmin app, 
or You can run the following command:
   ```shell
      docker exec postgres_container psql -U user -d netflix_show_db -f /app/create_table_relations.sql
   ```
This command executes the file `create_table_relations.sql` that applies a series of table `PRIMARY`and `FOREIGN` keys, 
as well as different `CASCADE` behaviours upon `DELETE` or `UPDATE` of parent tables. 
The resulting entity relationship diagram (ERD) is presented below:

![ERD](/doc/figures/ERD.png)

Data is now ready to be analyzed. 

## Business questions
Now that we have a structured database we can start running some queries over it to try to answer some business questions.
Below there is a list of questions we answer on this project. These are some illustrative business questions that are meant to simulate the need to query our database in very diverse ways. 
Obviously these are dependent on the type and availability of our data, this is way, for instance, in Q3 we limit our selves to query only "movies" as there is no data in minutes for "TV Series".

- **Q1**: _Which Actors Have Starred in the Most Shows?_
- **Q2**: _Number of Shows Released Each Year, by Genre_
  - **Q2b**: _Variation over the number of shows release each year?_
- **Q3**: _Distribution of Movie durations by content Rating_
- **Q4**: _Top Directors by Number of Shows and Average Duration_

 
 

To answer these questions we have writen a series of complex SQL queries stored in 
`business_questions.sql` file. You can execute this file by running the following command:
   ```shell
      docker exec postgres_container psql -U user -d netflix_show_db -f /app/business_questions.sql
   ```
This command will run multiple queries to answer these business questions and store the results
as a `Materialized view`.

## Export data
Now that our data has been through some essential cleaning and formatting we can export it so we can use it for generating Dynamic Dashboards in the next section.
We have prepared a sql script to export each table as a CSV file. However, let's first create a denormalize version of the database by running:

```shell
   docker exec postgres_container psql -U user -d netflix_show_db -f create_all_join_mat_view.sql
```

Then we can proceed with the exportation process by running the script use the following command:
```shell
   docker exec postgres_container psql -U user -d netflix_show_db -f /app/export_to_csv.sql
```
You should find all exported CSVs under `/repo_folder/SQL/exports`


## Dashboard - Looker Studio
**_Coming soon..._**

## Potential improvements
- Fine data quality checks via data scrappy/API: Despite data cleaning there are still a minor issues in data due to wrongly typed data at the moment of creation of the dataset. Usually this can be could be prevented by constraining how data is input. It is difficult to systematically identify all error types and then correct them.
- Add show rating. Either by data scrapping, using an API, or merging other datasets containing this data.
- Pipeline automation. All pipeline can be automized by triggering all steps at the moment of creation of the docker containers. Alternatively, we could use Airflow to handle the automation. However, given that all steps are run a single time, and given the showcasing style of this project, it appears excessive to implement this level of automation. 
- Security: In this project only the final database has a basic level of security. We could further enhance security by blocking access to MongoDB via password, creating different users and roles with varying degrees of data access. Also, currently there is no protected access to the PostgreSQL database if we intend to share it online, it would be necessary to create a basic SSL or TLS.
- Data redundancy: In this project there are no data replication strategies. If hardware is available we would ideally have some sort of data replication strategy to guarantee data availability and fault tolerance. Alternatively, data could be stored in cloud solutions such as AWS S3.
