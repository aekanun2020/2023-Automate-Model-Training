import datetime
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator

my_dag = DAG("dag_my_first_dag", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
             schedule_interval="@daily", catchup=False)
op = DummyOperator(task_id="task", dag=my_dag)