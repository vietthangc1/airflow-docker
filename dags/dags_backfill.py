from datetime import timedelta

import airflow
from airflow.decorators import task, dag

args = {
    'owner': 'vietthangc1',
    'start_date': airflow.utils.dates.days_ago(10),
    'dagrun_timeout': timedelta(minutes=60)
}

@dag(
    dag_id="dags_backfill_v2",
    schedule_interval="0 6 * * *",
    default_args=args,
    catchup=False
)
def introduction_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Kem",
            "last_name": "Tạ",
        }

    @task()
    def get_age():
        return 23

    @task()
    def say_hello(first_name, last_name, age):
        print(f"Hi! I'm {first_name} {last_name}. I'm {age} years old")

    name = get_name()
    age = get_age()
    hello = say_hello(first_name=name["first_name"],
                      last_name=name["last_name"],
                      age=age)

intro_dg = introduction_etl()