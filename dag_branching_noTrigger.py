# dags/branch_without_trigger.py
import pendulum

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

dag = DAG(
    dag_id="branch_without_trigger",
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 6, 19, tz="UTC"),
)

run_this_first = DummyOperator(task_id="run_this_first", dag=dag)
branching = BranchPythonOperator(
    task_id="branching", dag=dag, python_callable=lambda: "branch_a"
)

branch_a = DummyOperator(task_id="branch_a", dag=dag)
follow_branch_a = DummyOperator(task_id="follow_branch_a", dag=dag)

branch_false = DummyOperator(task_id="branch_false", dag=dag)

join = DummyOperator(task_id="join", dag=dag)

run_this_first >> branching
branching >> branch_a >> follow_branch_a >> join
branching >> branch_false >> join