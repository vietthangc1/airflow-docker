from datetime import timedelta

import airflow

import pandas as pd
from io import BytesIO

from airflow.models import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60),
    'catchup': False
}


def mysql_to_s3(query, ds_nodash):
    mysql_hook = MySqlHook(mysql_conn_id="mysql-airflow")
    connection = mysql_hook.get_conn()

    output = pd.read_sql(query, connection)
    data = BytesIO(output.to_csv(index=False).encode('utf-8'))

    filename = f"orders/{ds_nodash}.csv"

    s3_hook = S3Hook(aws_conn_id="s3-minio")
    s3_hook._upload_file_obj(file_obj=data,
                             key=filename,
                             bucket_name="airflow",
                             replace=True)


with DAG(
    dag_id="mysql_connector",
    schedule="@once",
    default_args=args,
) as dag:
    create_dags_table = MySqlOperator(
        task_id="create_dags_table",
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_dags (
            dt DATE NOT NULL,
            dag_id VARCHAR(255) NOT NULL);
        """,
        mysql_conn_id="mysql-airflow",
    )

    query_mysql = PythonOperator(
        task_id="query_mysql",
        python_callable=mysql_to_s3,
        op_kwargs={
            "query": "SELECT * FROM orders LIMIT 10;"
        }
    )

    # insert_dags_table = PostgresOperator(
    #     task_id="insert_dags_table",
    #     sql="""
    #         DELETE FROM airflow_dags WHERE dt = '{{ ds_nodash }}' AND dag_id = '{{ dag.dag_id }}';
    #         INSERT INTO airflow_dags (dt, dag_id) VALUES('{{ ds_nodash }}', '{{ dag.dag_id }}');
    #     """,
    #     postgres_conn_id="postgres-airflow",
    # )

    # create_dags_table >> insert_dags_table
