from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60),
    'catchup': False
}

with DAG(
    dag_id="postgres_connection_v2",
    schedule="@once",
    default_args=args,
) as dag:
    create_dags_table = PostgresOperator(
        task_id="create_dags_table",
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_dags (
            dt DATE NOT NULL,
            dag_id VARCHAR NOT NULL);
        """,
        postgres_conn_id="postgres-airflow",
    )

    insert_dags_table = PostgresOperator(
        task_id="insert_dags_table",
        sql="""
            DELETE FROM airflow_dags WHERE dt = '{{ ds_nodash }}' AND dag_id = '{{ dag.dag_id }}';
            INSERT INTO airflow_dags (dt, dag_id) VALUES('{{ ds_nodash }}', '{{ dag.dag_id }}');
        """,
        postgres_conn_id="postgres-airflow",
    )

    create_dags_table >> insert_dags_table 