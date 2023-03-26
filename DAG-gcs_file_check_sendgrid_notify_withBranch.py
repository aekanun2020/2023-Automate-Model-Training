import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from google.cloud import storage
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Set your GCS bucket and file path
GCS_BUCKET = 'airflow18mar2023'
GCS_OBJECT_PATH = 'rawdata.csv'

# Set your SendGrid API Key and email parameters
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = 'aekanun2020@gmail.com'
TO_EMAIL = 'aekanunbigdata@gmail.com'
EMAIL_SUBJECT = "GCS File Check Result"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime.now() - timedelta(minutes=1),
}

def check_gcs_object(bucket, object_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = storage.Blob(object_path, bucket)
    return blob.exists()

def send_email_via_sendgrid(file_found):
    if file_found:
        content = f"<h3>The file {GCS_OBJECT_PATH} was found in the GCS bucket {GCS_BUCKET}.</h3>"
    else:
        content = f"<h3>The file {GCS_OBJECT_PATH} was not found in the GCS bucket {GCS_BUCKET}.</h3>"

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=EMAIL_SUBJECT,
        html_content=content,
    )

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)

def decide_email_branch(**kwargs):
    ti = kwargs['ti']
    file_found = ti.xcom_pull(task_ids='check_file')
    return 'send_found_email' if file_found else 'send_not_found_email'

with DAG(
    "gcs_file_check_sendgrid_notify",
    default_args=default_args,
    description="DAG to check GCS file and send notification via SendGrid",
    schedule_interval="* * * * *",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    check_file = PythonOperator(
        task_id="check_file",
        python_callable=check_gcs_object,
        op_args=[GCS_BUCKET, GCS_OBJECT_PATH],
        provide_context=True,
    )

    decide_branch = BranchPythonOperator(
        task_id='decide_email_branch',
        python_callable=decide_email_branch,
        provide_context=True,
    )

    send_found_email = PythonOperator(
        task_id="send_found_email",
        python_callable=send_email_via_sendgrid,
        op_args=[True],
    )

    send_not_found_email = PythonOperator(
        task_id="send_not_found_email",
        python_callable=send_email_via_sendgrid,
        op_args=[False],
    )

    end = DummyOperator(task_id="end")

    start >> check_file >> decide_branch
    decide_branch >> send_found_email >> end
    decide_branch >> send_not_found_email >> end
