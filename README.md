# Flight Delay Prediction using Dataproc and Composer

This project utilizes Google Cloud Composer 1.20.12, Google Dataproc, and PySpark to ingest, process, and analyze airline delay data, ultimately building a machine learning model for predicting airline delays. 

## Table of Contents
- [Architecture](#architecture)
- [Components](#components)
  - [DAG-automateML_Notification.py](#dag-automateml_notificationpy)
  - [G-Student.py](#g-studentpy)
  - [refinedzone_H-Student.py](#refinedzone_h-studentpy)
- [Setup and Run](#setup-and-run)

## Architecture
This project is designed with an ETL (Extract, Transform, Load) framework in mind. The steps involved include:

1. **Ingestion**: Data is pulled from an SQL Server database, read using PySpark, and written in Parquet format to the raw zone in an HDFS (Hadoop Distributed File System).
2. **Data Transformation and Modeling**: PySpark is utilized for further data transformation, feature engineering tasks, and model training. The final model is saved to Google Cloud Storage.
3. **Orchestration**: Google Cloud Composer is used for scheduling and monitoring the data pipeline, and Google Dataproc is used for running the PySpark jobs.

## Components

### DAG-automateML_Notification.py
This file contains the Apache Airflow Directed Acyclic Graph (DAG) definition. The DAG represents a sequence of tasks with specified dependencies, forming a pipeline that ingests, transforms, and trains a model on airline data. The DAG is configured to run every 30 minutes. This file is created specifically to work with Google Cloud Composer, which is a fully managed workflow orchestration service built on Apache Airflow. The tasks in the DAG make use of Google Dataproc for running PySpark jobs. The DAG also includes task failure and success email notifications.

### G-Student.py
This PySpark script connects to a SQL Server database, reads the flight detail data, and writes the data in parquet format to the raw zone in an HDFS (Hadoop Distributed File System).

### refinedzone_H-Student.py
This PySpark script carries out further transformation and modeling tasks. The specific processes include:
- Reading the parquet data from the raw zone
- Data cleaning by filtering out cancelled flights, dropping null values, casting column types to their correct form
- Discretizing the 'DepTime' column, normalizing 'Distance' and 'ArrDelay' columns
- Splitting the data into training and test sets
- Transforming categorical variables into numerical ones
- Assembling all the features into a single vector
- Training a linear regression model using the training set
- Saving the trained model into Google Cloud Storage

## Setup and Run
1. Set up a Google Cloud Composer environment.
2. Place `G-Student.py` and `refinedzone_H-Student.py` in an accessible Google Cloud Storage bucket for the PySpark jobs.
3. Upload `DAG-automateML_Notification.py` to the DAGs folder in the Cloud Storage bucket associated with your Cloud Composer environment.
4. In the Cloud Composer web interface, activate the DAG.
5. The pipeline will begin to run according to the schedule specified in the DAG.

**Note**: Please ensure the setup of your Google Cloud Composer environment is correct and your environment has access to Google Cloud Storage and Google Dataproc. Also, you will need to adjust several parameters (like SQL server connection details and bucket names) within the scripts according to your specific environment and data.
