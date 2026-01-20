import logging
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="my_etl",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
)
def my_etl_pipeline():

    @task
    def extract_data():
        return [1,2,3]

    @task
    def transform_data(data):
        return [x*2 for x in data]

    @task
    def load_data(data):
        logging.info(data)

    data = extract_data()
    transformed = transform_data(data)
    load_data(transformed)

dag = my_etl_pipeline()
