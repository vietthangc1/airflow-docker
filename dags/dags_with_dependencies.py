from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator

args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60),
    'catchup': False
}

def get_sklearn_impl():
    import sklearn
    print("sklearn version: ", sklearn.__version__)

def get_matplotlib_impl():
    import matplotlib
    print("matplotlib version: ", matplotlib._version)

with DAG(
    dag_id="extending_airflow_dependencies_v1",
    schedule="@once",
    default_args=args,
) as dag:
    get_sklearn = PythonOperator(
        task_id="get_sklearn",
        python_callable=get_sklearn_impl
    )
    get_matplotlib = PythonOperator(
        task_id="get_matplotlib",
        python_callable=get_matplotlib_impl
    )
   