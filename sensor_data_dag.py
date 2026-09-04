from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import DAG
from airflow.models import Variable
from airflow.decorators import task, task_group
from datetime import timedelta

# path to python executable in the venv
python_exec_path = Variable.get("SENSORS_PYTHON_EXEC_PATH")

# path to the .env file
env_path = Variable.get("SENSORS_ENV_PATH")

# might make sense getting the date from an Airflow variable to handle any past dates
# TODO: might also make no sense at all, since someone IS going to forget that date there and fetch the same data over and over
date = Variable.get("SENSORS_DAG_DATE", None)

with DAG(
    "sensor_data_dag",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1)
    },
    description='Extract machine events from current sensor data',
    schedule="0 4 * * *"
) as dag:

    # read conf file and return a list of machine-specific config parameters for all machines defined in env.WORKCENTERS
    # with the "task expansion" or whatever this results in the subsequent tasks being replicated for each workcenter
    @task.external_python(python=python_exec_path, task_id="read_config_task")
    def read_config(env_path=None, date=None):
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)

        import json
        from datetime import datetime, timezone, timedelta
        from tuova_sensors.config import config

        # default to previous day if "SENSORS_DAG_DATE" variable not provided
        if date is None:
            date = (datetime.now(timezone.utc).astimezone() - timedelta(days=1)).date().isoformat()

        print(f"read_config for [{date}]")

        workcenters = json.loads(config["workcenters"])
        opts_list = []

        for i, w in enumerate(workcenters):
            opts = {
                "influx_measurement": w["influx_measurement"],
                "influx_field": w["influx_field"],
                "workcenter_id": w["workcenter_id"],
                # these are not machine-specific but whatever:
                "env_path": env_path,
                "date": date,
                # add a different "index" to each list entry to be safely used as part of the filenames
                "index": i
            }

            opts_list.append({"opts": opts})

        return opts_list

    # NOTE: all tasks from here on operate on a single "opts" object from the list returned by read_config

    # get the raw data from influx
    @task.external_python(python=python_exec_path, task_id="extract_data_task")
    def extract_data(opts=None):
        env_path = opts["env_path"]
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)

        import os
        from tuova_sensors.config import config
        from tuova_sensors.influx import get_data

        date = opts["date"]
        index = opts["index"]
        print(f"extract_data ({index}) for [{date}]...")

        # note to self: the "|" thing here works like the spread operator "{...config, ...opts}" in js
        # i.e. we add the relevant fields from env.WORKCENTERS item to the config
        real_opts = config | opts

        # should have the DATA folder at the "root" of the venv
        folder_path = os.path.join(os.path.dirname(env_path), "DATA")

        with open(f"{folder_path}/{date}-{index}.raw.csv", "w") as f:
            # do the influx api call (get_data returns a buffer)
            data_buf = get_data(date=date, opts=real_opts)
            # write buffer contents to csv file
            f.write(data_buf.getvalue())
            # need to close this one manually
            data_buf.close()

        return opts

    # do the summary
    @task.external_python(python=python_exec_path, task_id="transform_data_task")
    def transform_data(opts=None):
        env_path = opts["env_path"]
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)

        import os
        from tuova_sensors.config import config
        from tuova_sensors.summary import get_summary

        date = opts["date"]
        index = opts["index"]
        print(f"transform_data ({index}) for [{date}]...")

        # modify config with the machine-specific stuff again
        real_opts = config | opts

        # should again have the DATA folder at the "root" of the venv
        folder_path = os.path.join(os.path.dirname(env_path), "DATA")

        with open(f"{folder_path}/{date}-{index}.raw.csv", "r") as infile:
            # scope of the input csv file
            # get_summary returns a dict of dataframes, so serialize to parquet (can't really json.dumps these)
            # there's 3 of them so 3 files are needed (apparently can't do hdf5 since there are IntegerArrays)
            file_base = f"{folder_path}/{date}-{index}"
            results = get_summary(in_csv=infile, opts=real_opts)

            # TODO: if data is empty, should something happen here?
            try:
                results["df"].to_parquet(f"{file_base}.df.parquet")
                results["segments"].to_parquet(f"{file_base}.segments.parquet")
                results["totals"].to_parquet(f"{file_base}.totals.parquet")
            except Exception as ex:
                # next task starts just fine
                print("transform_data - there's probably not much data to transform")
                print("transform_data - here's the real error though: " + str(ex))

        return opts

    # send the summary to postgresql
    @task.external_python(python=python_exec_path, task_id="load_data_task")
    def load_data(opts=None):
        env_path = opts["env_path"]
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)

        import os
        import pandas as pd
        from tuova_sensors.config import config
        from tuova_sensors.postgres import insert_summary

        date = opts["date"]
        index = opts["index"]
        print(f"load_data ({index}) for [{date}]...")

        # modify config with the machine-specific stuff againgain
        real_opts = config | opts

        # should once again have the DATA folder at the "root" of the venv
        folder_path = os.path.join(os.path.dirname(env_path), "DATA")

        # get the parquets
        file_base = f"{folder_path}/{date}-{index}"

        # TODO: these would be missing if there's no data for the day, so what should happen?
        try:
            df = pd.read_parquet(f"{file_base}.df.parquet")
            segments = pd.read_parquet(f"{file_base}.segments.parquet")
            totals = pd.read_parquet(f"{file_base}.totals.parquet")

            # do the postgres call
            # TODO: remove dry_run
            result = insert_summary(data={"df": df, "segments": segments, "totals": totals}, opts=real_opts, dry_run=True)
            print(result)
        except Exception as ex:
            print("load_data - there's probably nothing much to load, see that error below?")
            print("load_data: " + str(ex))

        return opts

    # remove all files created by the previous tasks
    @task.external_python(python=python_exec_path, task_id="cleanup_task")
    def cleanup(opts=None):
        env_path = opts["env_path"]
        import os
        import glob

        date = opts["date"]
        index = opts["index"]
        print(f"cleanup ({index}) for [{date}]...")

        # should definitely at this point have the DATA folder at the "root" of the venv
        folder_path = os.path.join(os.path.dirname(env_path), "DATA")
        for f in glob.glob(f"{folder_path}/{date}-{index}*"):
            print(f"delete {f}")
            # TODO: uncomment below to actually remove the files
            #os.unlink(f)

        return opts

    @task_group
    def group(opts=None):
        extract_data(opts=opts) >> transform_data(opts=opts) >> load_data(opts=opts) >> cleanup(opts=opts)

    group.expand_kwargs(read_config(env_path=env_path, date=date))
