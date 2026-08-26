from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from datetime import timedelta

def extract_data():
    print("extract_data...")

def transform_data():
    print("transform_data...")

def load_data():
    print("load_data...")

with DAG(
    "sensor_data_dag",
    default_args={
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1)
    },
    description="Extract machine events from current sensor data",
    schedule="0 3 * * *"
) as dag:
    extract_data_task = PythonOperator(task_id="extract_data_task", python_callable=extract_data)
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform_data)
    load_data_task = PythonOperator(task_id="load_data_task", python_callable=load_data)

    extract_data_task >> transform_data_task >> load_data_task
