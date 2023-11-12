[![CI](https://github.com/nogibjj/xueqing_wu_individual3/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/xueqing_wu_individual3/actions/workflows/cicd.yml)

## Purpose
The goal of this project is to build an ETL pipeline on Databricks. 

## Funciontality


## Dataset
The two datasets are the number of births for each year, month, and day. Below are data sources:
1. https://github.com/fivethirtyeight/data/blob/master/births/US_births_2000-2014_SSA.csv
1. https://github.com/fivethirtyeight/data/blob/master/births/US_births_1994-2003_CDC_NCHS.csv

## Steps
1. Data Extraction:

  Utilizes the requests library to fetch birth data from specified URLs.
  
  Downloads and stores the data in the Databricks FileStore.

2. Databricks Environment Setup:

  Establishes a connection to the Databricks environment using environment variables for authentication (SERVER_HOSTNAME and ACCESS_TOKEN).

3. Data Transformation and Load

Transform the csv file into a Spark dataframe which is then converted into a Delta Lake Table and stored in the Databricks environement

4. Query Transformation and Vizulization:

Defines a Spark SQL query to perform a predefined transformation on the retrieved data.

Uses the predifined transformation Spark dataframe to create vizualizations

5. File Path Checking for make test:

Implements a function to check if a specified file path exists in the Databricks FileStore.

As the majority of the functions only work exclusively in conjunction with Databricks, the Github environment cannot replicate and do not have access to the data in the Databricks workspace. I have opted to test whether I can still connect to the Databricks API.

Utilizes the Databricks API and the requests library.

6. Automated trigger via Github Push:*

I utilize the Databricks API to run a job on my Databricks workspace such that when a user pushes to this repo it will intiate a job run

Preparation:

1. Create a Databricks workspace on Azure
1. Connect Github account to Databricks Workspace (user settings-->Linked Accounts)
1. Create global init script for cluster start to store enviornment variables (Admin Settings-->Global Init) (Include SERVER_HOSTNAME, ACCESS_TOKEN)
1. Create a Databricks cluster that supports Pyspark
1. Clone repo into Databricks workspace
1. Create a job on Databricks to build pipeline
    1. Extract task (Data Source): mylib/extract.py
    1. Transform and Load Task (Data Sink): mylib/transform_load.py
    1. Query and Viz Task: mylib/query_viz.py

## Databricks ETL Pipeline
<img width="648" alt="Databricks ETL Pipeline" src="https://github.com/nogibjj/xueqing_wu_individual3/assets/47194238/63eb200a-bc6e-4a64-882c-74eddbda4545">

## Data Visualization
<img width="847" alt="Bar Graph" src="https://github.com/nogibjj/xueqing_wu_individual3/assets/47194238/c9079981-33e9-4c08-8798-5485ce10e797">

<img width="849" alt="histogram" src="https://github.com/nogibjj/xueqing_wu_individual3/assets/47194238/7b6b9786-0511-485a-8e74-77ffaf8c47ec">


## References

1. https://github.com/nogibjj/python-ruff-template
1. https://docs.databricks.com/en/getting-started/data-pipeline-get-started.html



