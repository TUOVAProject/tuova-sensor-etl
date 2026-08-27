# tuova_sensors

## Overview

Python scripts for an ETL process that does the following:

1. Get raw sensor data from an influxdb database (Extract)
2. Calculate summaries and aggregations (Transform)
3. Send data to a postgresql data warehouse (Load).

Includes a DAG for Apache Airflow.

## Dev setup

1. Clone the project
2. Create virtual environment with `uv init` or `python3 -m venv .venv`
3. Install dependencies with `uv sync` or `pip install -r requirements.txt`
4. Create .env with `cp .env.example .env` and modify as needed
5. Modify and run example visualization with `python -m scripts.test`
6. ???

## Deployment setup

### General

The DAG uses Airflow's `ExternalPythonOperator` running in a virtual environment to keep project dependencies from interfering with Airflow's own dependencies (as well as any other projects'). The DAG file reads 2 variables from Airflow's variables (define these beforehand as per step 5 below):

- `SENSORS_ENV_PATH`: path to the `.env` file containing further variables needed by tasks
- `SENSORS_PYTHON_EXEC_PATH`: path to the virtual environment's `python` interpreter.

This setup is based on Airflow's example `docker-compose.yml` file that defines multiple containers for different services. All Airflow-specific volumes are assumed to be bind mounts that are shared between Airflow containers (e.g. `airflow-dag-processor`, `airflow-worker` etc.).

The virtual environment is created under Airflow's `config` folder, which in this case is expected to be mounted to the containers as follows:

`/home/user/projects/airflow/data/config:/opt/airflow/config`

An example deployment process would go as follows (adjust the paths as needed):

### The virtual environment

1. Access the Airflow host machine (e.g. with `ssh`)
2. Access Airflow container with `docker exec -it airflow-dag-processor bash` (adjust container name as needed)
3. Create directory for the virtual environment with `mkdir -p /opt/airflow/config/python-venvs/tuova_sensors`
4. Create the virtual environment with `cd /opt/airflow/config/python-venvs/tuova_sensors && python -m venv .venv`
5. On the local machine, bundle the required files to `./build` by running `./build.sh`
6. Copy contents of the `./build` directory to the Airflow host machine, e.g.:

`scp -r ./build/* user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/tuova_sensors/`
`scp ./build/.env user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/tuova_sensors/`

(it's likely that a wild `permission denied` appears, in that case either `chown` or `chmod` the target folder as appropriate, or `scp` the stuff to somewhere else first, then `sudo cp` to the destination folder on the target host)

7. Access the Airflow host machine and Airflow container again as per steps 1 and 2 above, then `cd /opt/airflow/config/python-venvs/tuova_sensors`
8. Activate the python virtual environment with `source ./.venv/bin/activate`
9. Install the copied python modules and their dependencies to the virtual environment with `pip install -e .`

### The DAG

10. Login to the Airflow UI (or use the cli?) and define the following variables (Admin -> Variables):

- `SENSORS_ENV_PATH` = `/opt/airflow/config/python-venvs/tuova_sensors/.env`
- `SENSORS_PYTHON_EXEC_PATH` = `/opt/airflow/config/python-venvs/tuova_sensors/.venv/bin/python`

11. ???

## Configuration

- ??!!

```bash
uv export --no-dev --no-hashes --no-annotate --no-header --format requirements-txt > requirements.txt
```
