from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:
    task_1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task_2 = BashOperator(
        task_id="sleep",
        bash_command="sleep 5"
    )

    task_1 >> task_2
