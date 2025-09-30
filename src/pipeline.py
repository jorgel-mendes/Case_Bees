from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.fetch import fetch_breweries, save_raw_data
from src.transform import transform_to_silver
from src.aggregate import aggregate_to_gold

dag = DAG(
    'breweries_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
)

def fetch_bronze():
    data = fetch_breweries()
    save_raw_data(data, 'data/bronze/breweries_raw.json')

def transform_silver():
    transform_to_silver('data/bronze/breweries_raw.json', 'data/silver/breweries_silver.parquet')

def aggregate_gold():
    aggregate_to_gold('data/silver/breweries_silver.parquet', 'data/gold/breweries_agg.parquet')

fetch_task = PythonOperator(
    task_id='fetch_bronze',
    python_callable=fetch_bronze,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_gold',
    python_callable=aggregate_gold,
    dag=dag,
)

fetch_task >> transform_task >> aggregate_task