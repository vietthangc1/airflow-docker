from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60),
    'catchup': False
}


with DAG(
    dag_id='dag_with_minio_s3_v02',
    schedule_interval='@daily',
    default_args=default_args,
) as dag:
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='s3-minio',
        mode='poke',
        poke_interval=5,
        timeout=30
    )