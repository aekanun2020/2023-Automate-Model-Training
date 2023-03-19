from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowException
import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

from google.cloud import secretmanager
import os

PROJECT_ID = "skilful-boulder-381002"
CLUSTER_NAME = "aekanun-arrdelay-regressionmodel"
REGION = "us-east1"
ZONE = "us-east1-a"
PYSPARK_URI_MSSQL_TO_HDFS = "gs://airflow18mar2023/G-Student.py"
PYSPARK_URI_HDFS_TO_MODEL = "gs://airflow18mar2023/refinedzone_H-Student.py"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': YESTERDAY,
}

from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    image_version='1.5-centos8',
    num_workers=2,
    master_machine_type="n1-standard-2",
    master_disk_type="pd-standard",
    master_disk_size=500,
    worker_machine_type="n1-standard-2",
    worker_disk_type="pd-standard",
    worker_disk_size=500,
    properties={"spark:spark.jars.packages": "com.microsoft.azure:spark-mssql-connector:1.0.2",
                "dataproc:dataproc.conscrypt.provider.enable": "false"}
).make()

def send_email_on_failure(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    email = EmailOperator(
        task_id="send-email-failure",
        conn_id="sendgrid_default",
        to="aekanun@imcinstitute.com",
        subject=f"Failure - Pipeline of Automate model training: Task {task_id}",
        html_content=f"Failure - Pipeline of Automate model training. Task {task_id} failed.",
    )
    email.execute(context)

def send_email_on_success(context):
    email = EmailOperator(
        task_id="send-email-success",
        conn_id="sendgrid_default",
        to="aekanun@imcinstitute.com",
        subject="Success - Pipeline of Automate model training",
        html_content="Success - Pipeline of Automate model training. The Dataproc cluster has been completely deleted, and it is ready to deploy the ML model.",
    )
    email.execute(context)

with models.DAG(
    "MSSQL-Spark-HDFS-SparkML-GCS",
    catchup=False,
    schedule_interval=datetime.timedelta(minutes=30),
    default_args=default_dag_args,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        on_failure_callback=send_email_on_failure,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        on_failure_callback=send_email_on_failure,
        on_success_callback=send_email_on_success,
    )

    PYSPARK_JOB_MSSQL_TO_HDFS = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI_MSSQL_TO_HDFS},
    }

    PYSPARK_JOB_HDFS_TO_MODEL = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI_HDFS_TO_MODEL},
    }

    pyspark_task_mssql_to_hdfs = DataprocSubmitJobOperator(
        task_id="pyspark_task_mssql_to_HDFS",
        job=PYSPARK_JOB_MSSQL_TO_HDFS,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=send_email_on_failure,
    )

    pyspark_task_hdfs_to_model = DataprocSubmitJobOperator(
        task_id="pyspark_task_HDFS_to_Model",
        job=PYSPARK_JOB_HDFS_TO_MODEL,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=send_email_on_failure,
    )

    create_cluster >> pyspark_task_mssql_to_hdfs >> pyspark_task_hdfs_to_model >> delete_cluster

