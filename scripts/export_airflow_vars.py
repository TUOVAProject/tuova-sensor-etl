import json
from dotenv import load_dotenv

load_dotenv()

# print config as json for importing to airflow's variables
# NOTE:
# probably simpler to only add SENSORS_PYTHON_EXEC_PATH to airflow and have everything else in .env inside the .venv
#

from tuova_sensors.config import config

airflow_vars = {
    "SENSORS_INFLUX_URL": config["influx_url"],
    "SENSORS_INFLUX_ORG": config["influx_org"],
    "SENSORS_INFLUX_BUCKET": config["influx_bucket"],
    "SENSORS_INFLUX_MEASUREMENT": config["influx_measurement"],
    "SENSORS_INFLUX_TOKEN": config["influx_token"],
    "SENSORS_POSTGRES_HOST": config["pg_host"],
    "SENSORS_POSTGRES_PORT": config["pg_port"],
    "SENSORS_POSTGRES_USER": config["pg_user"],
    "SENSORS_POSTGRES_PASS": config["pg_pass"],
    "SENSORS_POSTGRES_DB": config["pg_db"],
    "SENSORS_EVENT_WORKCENTER_ID": config["event_workcenter_id"],
    "SENSORS_PYTHON_EXEC_PATH": config["dag_python_exec_path"],
    "SENSORS_ENV_PATH": config["dag_env_path"],
}

print(json.dumps(airflow_vars))
