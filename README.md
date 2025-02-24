# Data warehousing + ETL

## Introduction
In this project we will answer some business questions regarding (#todo: topic) . To this end, we will build all the necessary infrastructure to gather (Extract) data from multiple sources, normalize and clean it (Transform) and store it into a data warehouse.

This project will be complemented with subsequent projects that will leverage final outputs obtained here.

The objectives of this project are:
- Build a data warehouse solution that is scalable and can be easily deployed
- Develop an automated ETL pipeline that recollects data, process it and stores it daily.

The technologies required to set up the baseline structure are:
- Docker: To ensure reproducibility of project in different machines and to facilitate its deployment
- (#todo: warehouse solution)
- Apache Airflow: to orchestrate and automate the different steps included in the data pipelines


## Setup environment
### Python environment
- Download the `requirements.txt` file to setup the python environment.
- Once you created a new python virtual environment, use `source path/to/python/env/bin/active`
- Install dependencies using `pip install -r path/to/requirements.txt`

### Docker containers
- (Install docker)
- Start the containers using `docker-compose up -d`
- Verify that containers are running by using `docker ps`. You should see 2 containers.
