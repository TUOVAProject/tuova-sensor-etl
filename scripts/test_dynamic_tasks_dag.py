from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.decorators import task, task_group
from datetime import timedelta

global_var = "GLOBAL_VAR"

with DAG(
    "test_dynamic_tasks_dag",
        default_args={
            "depends_on_past": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=1)
        },
        description='Try to figure out how "task expansion" (or whatever) works',
        schedule="0 4 * * *"
) as dag:

    @task
    def read_config(global_var=None):
        print(global_var)
        return [
            {
                "global_var": global_var,
                "config": "CONFIG1"
            },
            {
                "global_var": global_var,
                "config": "CONFIG2"
            }
        ]

    @task
    def task1(global_var, config):
        print(f'task 1: {config}')
        print(global_var)
    
    @task
    def task2(global_var, config):
        print(f'task 2: {config}')
        print(global_var)

    @task_group
    def group(global_var, config):
        task1(global_var=global_var, config=config) >> task2(global_var=global_var, config=config)

    group.expand_kwargs(read_config(global_var=global_var))
