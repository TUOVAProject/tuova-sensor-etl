from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG
from airflow.models import Variable
from datetime import timedelta


python_exec_path = Variable.get("SENSORS_PYTHON_EXEC_PATH")
env_path = Variable.get("SENSORS_ENV_PATH")

###
def extract_data(env_path=None):
    # get the raw data from influx

    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)

    import os
    from datetime import datetime, timedelta, timezone
    from tuova_sensors.config import config
    from tuova_sensors.influx import get_data

    # use the previous day
    date = (datetime.now(timezone.utc).astimezone() - timedelta(days=1)).date().isoformat()

    print(f"extract_data [{date}]...")
    # should have the DATA folder at the "root" of the venv
    folder_path = os.path.join(os.path.dirname(env_path), "DATA")

    with open(f"{folder_path}/{date}.raw.csv", "w") as f:
        # do the influx api call (get_data returns a buffer)
        data_buf = get_data(date=date, opts=config)
        # write buffer contents to csv file
        f.write(data_buf.getvalue())
        # need to close this one manually
        data_buf.close()

    # pass the date to the next task through op_kwargs
    return date

###
def transform_data(env_path=None, date=None):
    # do the summary

    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)

    import os
    from tuova_sensors.config import config
    from tuova_sensors.summary import get_summary

    print(f"transform_data [{date}]...")
    # should again have the DATA folder at the "root" of the venv
    folder_path = os.path.join(os.path.dirname(env_path), "DATA")

    with open(f"{folder_path}/{date}.raw.csv", "r") as infile:
        # scope of the input csv file
        # get_summary returns a dict of dataframes, so serialize to parquet (can't really json.dumps these)
        # there's 3 of them so 3 files are needed (apparently can't do hdf5 since there are IntegerArrays)
        # oh well
        file_base = f"{folder_path}/{date}"
        results = get_summary(in_csv=infile, opts=config)

        results["df"].to_parquet(f"{file_base}.df.parquet")
        results["segments"].to_parquet(f"{file_base}.segments.parquet")
        results["totals"].to_parquet(f"{file_base}.totals.parquet")

    # pass the date to the next task through op_kwargs
    return date

###
def load_data(env_path=None, date=None):
    # send the summary to postgresql

    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)

    import os
    import pandas as pd
    from tuova_sensors.config import config
    from tuova_sensors.postgres import insert_summary

    # TODO:
    print(f"load_data [{date}]...")
    # should again have the DATA folder at the "root" of the venv
    folder_path = os.path.join(os.path.dirname(env_path), "DATA")

    # get the parquets
    file_base = f"{folder_path}/{date}"

    df = pd.read_parquet(f"{file_base}.df.parquet")
    segments = pd.read_parquet(f"{file_base}.segments.parquet")
    totals = pd.read_parquet(f"{file_base}.totals.parquet")

    # do the postgres call
    result = insert_summary(data={"df": df, "segments": segments, "totals": totals}, opts=config, dry_run=True)
    print(result)

    # pass the date to the next task through op_kwargs
    return date

###
def cleanup(env_path=None, date=None):
    # remove all files created by the current run tasks
    # TODO:
    print(f"cleanup [{date}]...")

###

# define the DAG
with DAG(
    "sensor_data_dag",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1)
    },
    description="Extract machine events from current sensor data",
    schedule="0 4 * * *" # TODO: get from airflow.models.Variable?
) as dag:
    # note those op_kwargs at the end
    extract_data_task = ExternalPythonOperator(python=python_exec_path, task_id="extract_data_task", python_callable=extract_data, op_kwargs={"env_path": env_path})
    transform_data_task = ExternalPythonOperator(python=python_exec_path, task_id="transform_data_task", python_callable=transform_data, op_kwargs={"env_path": env_path, "date": extract_data_task.output})
    load_data_task = ExternalPythonOperator(python=python_exec_path, task_id="load_data_task", python_callable=load_data, op_kwargs={"env_path": env_path, "date": transform_data_task.output})
    cleanup_task = ExternalPythonOperator(python=python_exec_path, task_id="cleanup_task", python_callable=cleanup, op_kwargs={"env_path": env_path, "date": load_data_task.output})

    # weird Airflow process pipeline task order definition syntax thingy
    extract_data_task >> transform_data_task >> load_data_task >> cleanup_task
