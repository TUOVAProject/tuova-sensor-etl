import os

config = {
    # summary:
    "states": {
        -1: "LOW_ERR",
        0: "OFF",
        1: "IDLE",
        2: "PROCESSING",
        3: "HIGH_ERR"
    }, 
    "thresholds": [
        0,
        0.8,
        6.5,
        100
    ],
    "debounce_up_sec": 15, 
    "debounce_down_sec": 30,

    # influxdb:
    "influx_url": os.getenv("INFLUX_URL"),
    "influx_org": os.getenv("INFLUX_ORG"),
    "influx_bucket": os.getenv("INFLUX_BUCKET"),
    "influx_token": os.getenv("INFLUX_TOKEN"),

    # postgres:
    "pg_host": os.getenv("POSTGRES_HOST"),
    "pg_port": os.getenv("POSTGRES_PORT"),
    "pg_user": os.getenv("POSTGRES_USER"),
    "pg_pass": os.getenv("POSTGRES_PASS"),
    "pg_db": os.getenv("POSTGRES_DB"),

    # (data model v1.2)
    # TODO: get these from DB?
    "event_type_ids": {
        0: 16,  # "POWER OFF"
        1: 14,  # "MACHINE IDLE"
        2: 11,   # "MACHINE PRODUCTION"
        9999: 17  # "MACHINE DAY"
    },
    "event_source_id": 3,
    "LOCALE": "Europe/Helsinki",
    "dag_python_exec_path": os.getenv("DAG_PYTHON_EXEC_PATH"),
    "dag_env_path": os.getenv("DAG_ENV_PATH"),
    # mapping of workcenters to influxdb _measurement._field
    # WORKCENTERS env variable structure:
    # [{"workcenter_id": "WORKCENTER_ID", "influx_measurement": "INFLUX_MEASUREMENT", "influx_field": "INFLUX_FIELD"}]
    "workcenters": os.getenv("WORKCENTERS"),
}
