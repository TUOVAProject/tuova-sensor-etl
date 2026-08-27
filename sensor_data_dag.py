from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG
from airflow.models import Variable
from datetime import datetime, timedelta

python_venv_path = Variable.get("SENSORS_PYTHON_EXEC_PATH")
env_path = Variable.get("SENSORS_ENV_PATH")

def extract_data(env_path=None):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    from tuova_sensors.config import config
    #from influx import get_data
 
    print("INFLUX_URL = " + config["influx_url"])
    date = "2026-08-03" #datetime.now().date().isoformat()
    print(f"extract_data for [{date}]...")
    #print(config["influx_url"])
    #data = get_data(date=date, opts=config)

###

def transform_data():
    print("transform_data...")

def load_data():
    print("load_data...")

with DAG(
    "sensor_data_dag",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1)
    },
    description="Extract machine events from current sensor data",
    schedule="0 4 * * *"
) as dag:
    extract_data_task = ExternalPythonOperator(python=python_venv_path, task_id="extract_data_task", python_callable=extract_data, op_kwargs={"env_path": env_path})
    transform_data_task = ExternalPythonOperator(python=python_venv_path, task_id="transform_data_task", python_callable=transform_data)
    load_data_task = ExternalPythonOperator(python=python_venv_path, task_id="load_data_task", python_callable=load_data)

    extract_data_task >> transform_data_task >> load_data_task
