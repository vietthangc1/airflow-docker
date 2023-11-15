import os
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

default_args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60),
    'catchup': False,
}

AZURE_CONTAINER_NAME = 'knime'
PATH_TO_UPLOAD_FILE = '/data/data.csv'
AZURE_BLOB_NAME = 'airflow/test.csv'
BLOB_CONN_ID = 'FH-BlobStorage'

with DAG(
    dag_id='dags_upload_blob_storage',
    schedule_interval='@daily',
    default_args=default_args,
) as dag:
    upload = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path=PATH_TO_UPLOAD_FILE,
        container_name=AZURE_CONTAINER_NAME,
        blob_name=AZURE_BLOB_NAME,
        wasb_conn_id=BLOB_CONN_ID,
    )