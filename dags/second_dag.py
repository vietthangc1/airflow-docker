"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def say_hello(ti):
    name = ti.xcom_pull(task_ids='run_this_first', key='name')
    age = ti.xcom_pull(task_ids='run_this_first', key='age')
    print(f"Hello! I'm {name}, {age} years old.")

def return_info(ti):
    ti.xcom_push(key='name', value='Kem')
    ti.xcom_push(key='age', value=23)

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='python_operator_v3',
    default_args=args,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = PythonOperator(
    task_id='run_this_last',
    dag=dag,
    python_callable=say_hello,
)

run_this_first = PythonOperator(
    task_id='run_this_first',
    dag=dag,
    python_callable=return_info
)

run_this_first >> run_this_last

if __name__ == "__main__":
    dag.cli()