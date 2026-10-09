# tuova_sensors

> ## IMPORTANT!
>
> This repository was created as part of the TUOVA Project (see https://tuova.savonia.fi and https://tuovaproject.github.io/). Some of the information presented here might assume familiarity with other repositories created during the course of the project. For more details on those, see the "Additional Information" section at the bottom of this README.
>
> **Everything is provided as-is, use at your own risk.**
>
> TUOVA Project co-funded by the European Union / Etelä-Savon ELY-keskus.
>
> ![Co-funded by the EU](co-fundedbythe-eu.png)

## General

This repository contains python modules for an ETL process that does the following:

1. Get raw sensor data from an InfluxDB (v.2) database (Extract)
2. Calculate summaries and aggregations (Transform)
3. Send data to a PostgreSQL database (Load).
4. (optionally visualize aggregated data with matplotlib)

While these modules do not specifically assume a certain way of collecting the raw sensor data, it is somewhat assumed that said data consists of electric current measurements taken from the mains supply of a multi-function CNC machine (1 phase, ranging from 0A to 100A, measured in 1-second intervals).

An Apache Airflow DAG file is also provided, see https://airflow.apache.org/ for further info.

## Prerequisites

- It is assumed that an InfluxDB database with raw measurement data is already deployed
- It is also assumed that a PostgreSQL database with the data model schema is already deployed
- While not mandatory, any Airflow-related info assumes that Airflow is already deployed.

## Dev setup

1. Clone the project
2. Create Python virtual environment (e.g. `uv init` or `python3 -m venv .venv`)
3. Activate the virtual environment (`source ./.venv/bin/activate`)
4. Install dependencies (e.g. `uv sync`)
5. Create `config.json` as per `config.example.json` (`cp ./config.example.json ./config.json`)
6. Modify the config file as needed
7. Run example visualization with `python ./test.py`.

## Configuration parameters

- `influx_*`: Your InfluxDB database details
- `pg_*`: Your PostgreSQL database details
- `workcenters`: An array of workcenters to which the process would be applied. Fields:
    - `workcenter_id`: The unique identifier of this workcenter/machine (e.g. from ERP)
    - `influx_measurement`: Name of the InfluxDB measurement containing raw sensor data for this workcenter (e.g. `tuova-iot`)
    - `influx_field`: Name of the InfluxDB field containing raw sensor data for this workcenter (e.g. `L1`)
    - `states`: Parameters for the different states (`OFF`, `IDLE`, `PROCESSING`) of this workcenter. The structure is as follows:
        - `[STATE]`:
            - `id`: Internal ID for the state `[STATE]`, used as an index in calculations (just leave these to `OFF=0`, `IDLE=1`, `PROCESSING=2`)
            - `min`: The lower value boundary / threshold of `[STATE]`
            - `max`: The higher value boundary / threshold of `[STATE]`
            - (put simply, a given measurement maps to `[STATE]` whenever its value is between `min` and `max`)
    - `debounce_up_sec`: Minimum time in seconds for which the measurement needs to be between `min` and `max` to register a state change from a "lower" state (i.e. a state with a smaller `id`) to a higher one
    - `debounce_down_sec`: Same as above, but the other way around (i.e. when transitioning from a "higher" state to a "lower" one)
- `event_type_ids`: Mappings between states' internal IDs and event types in PostgreSQL (example values based on the data model). Internal ID of `9999` is used for daily summaries.
- `event_source_id`: Source identifier in PostgreSQL (`3` meaning events are based on sensor data, as per the data model)
- `LOCALE`: Controls the timezone of local datetimes. Use `Europe/Helsinki` if summaries etc. are supposed to be based on Finnish time (should take DST into account as well).

It's worth noting that `LOCALE` ultimately affects the following values of the data model: `start_at`, `end_at`, `start_date`, `end_date`, `start_time` and `end_time`. If `LOCALE` is left unset, Python defaults to system time which could be different on your dev machine than what it would be on the server (e.g. Docker defaults to UTC). So just set this to the same timezone as your dev machine to avoid the usual problems in deployment.

## Deployment setup

### General

The DAG uses Airflow's `ExternalPythonOperator` running in a virtual environment to keep project dependencies from interfering with Airflow's own dependencies (as well as any other projects'). The DAG file reads 2 variables from Airflow's variables (define these beforehand as per step 5 below):

- `SENSORS_CONFIG_PATH`: path to the `config.json` file (e.g. in the root of the venv)
- `SENSORS_PYTHON_EXEC_PATH`: path to the virtual environment's `python` interpreter.

The setup used here is based on Airflow's example `docker-compose.yml` file that defines multiple containers for all the different services. Here all the Airflow-specific volumes are assumed to be bind mounts that are shared between Airflow containers (e.g. `airflow-dag-processor`, `airflow-worker` etc.).

The virtual environment is created under Airflow's `config` folder, which in this case is expected to be mounted to the containers as follows (e.g. in `docker-compose.yml`):

`/home/user/projects/airflow/data/config:/opt/airflow/config`

An example deployment process would go as follows (adjust the paths and other info as needed):

### The virtual environment

1. Access the Airflow host machine (e.g. `ssh user@host`)
2. Access an Airflow container's shell (e.g. `docker exec -it airflow-dag-processor bash`, adjust container name as needed)
3. Create a directory for the virtual environment (e.g. `mkdir -p /opt/airflow/config/python-venvs/tuova_sensors/DATA`)
4. Create the virtual environment (e.g. `cd /opt/airflow/config/python-venvs/tuova_sensors && python -m venv .venv`)
5. On your dev machine, bundle the required files to `./build` by running `./build.sh`
6. Copy contents of the `./build` directory to the Airflow host machine, e.g.:

```bash
scp -r ./build/* user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/tuova_sensors/
```

(it's likely that a wild `permission denied` appears, in that case either `chown` or `chmod` the target folder as appropriate, or `scp` the stuff to somewhere else first, then `sudo cp` to the destination folder on the target host)

7. Access the Airflow host machine and Airflow container again as per steps 1 and 2 above and enter the venv directory (e.g. `cd /opt/airflow/config/python-venvs/tuova_sensors`)
8. In the venv directory, activate the python virtual environment (e.g. `source ./.venv/bin/activate`)
9. Install the copied python modules and their dependencies to the virtual environment (e.g. `pip install -e .`)

### The DAG

10. Login to the Airflow web-GUI (or use the CLI inside the container) and define the following variables (`Admin` -> `Variables` in the GUI):

- `SENSORS_CONFIG_PATH` = e.g. `/opt/airflow/config/python-venvs/tuova_sensors/config.json`
- `SENSORS_PYTHON_EXEC_PATH` = e.g. `/opt/airflow/config/python-venvs/tuova_sensors/.venv/bin/python`

11. Copy the DAG file (`./sensor_data_dag.py`) to the Airflow environment (e.g. with `scp` or just `nano` + copypaste)
12. Re-parse the DAGs in Airflow (either the `Reparse Dag` button in the GUI, or `airflow dags list` in the CLI)

Pro Tip: if the DAG is not visible in the web GUI, access the Airflow container shell and run `airflow dags list-import-errors` to hopefully get an idea what might be wrong.

## Notes

If nothing in this README (or the code) makes sense, feel free to improve.

## LICENCE

See [LICENCE.md](LICENCE.md)

## Additional Information

### The TUOVA Project

- https://tuova.savonia.fi
- https://tuovaproject.github.io/

### The data model ("Tuotantotiedon tietomalli")

- https://github.com/TUOVAProject/tuotantotiedon-tietomalli
- https://tuovaproject.github.io/tuotantotiedon-tietomalli/

### On-premises server environment

- https://github.com/TUOVAProject/tuova_on_prem_server

### Sensor data collection system

- https://github.com/TUOVAProject/tuova-iot

### Energy analyzer data collection system

- https://github.com/TUOVAProject/tuova_energymeter

### Sensor data ETL process

- https://github.com/TUOVAProject/tuova-sensor-etl

### Sensor data - further processing

- https://github.com/TUOVAProject/tuova_machinedata_processing
