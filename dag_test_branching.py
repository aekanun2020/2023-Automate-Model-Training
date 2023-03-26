import datetime
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.xcom import XCom


def branch_func(ti):
    xcom_value = int(ti.xcom_pull(task_ids="start_task"))
    if xcom_value >= 5:
        return "continue_task"
    else:
        return "stop_task"


dag = DAG("dag_test_branching", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
             schedule_interval="@daily", catchup=False)

start_op = BashOperator(
    task_id="start_task",
    bash_command="echo 5",
    do_xcom_push=True,
    dag=dag,
)

branch_op = BranchPythonOperator(
    task_id="branch_task",
    python_callable=branch_func,
    dag=dag,
)

continue_op = DummyOperator(task_id="continue_task", dag=dag)

stop_op = DummyOperator(task_id="stop_task", dag=dag)

start_op >> branch_op >> [continue_op, stop_op]