import os
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from lib.service import upload_events, get_all_processed_files
from lib.configs import configs


dag_name = os.path.basename(__file__).replace('.py', '')
default_args = {
    'start_date': datetime(2024, 1, 1),
}
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)


shared_dir_path = configs['shared_dir_path']


def upload_events_wrapper(shared_dir_path: str) -> None:
    """Wrapper function for uploading events to the database

    params:
    shared_dir_path - path to the directory containing event files

    The function:
    1. Gets list of processed files
    2. Uploads events from unprocessed files to the database
    """
    processed_files = get_all_processed_files()
    upload_events(shared_dir_path, processed_files)


with DAG(
    dag_name,
    default_args=default_args,
    description='write events to csv file in shared directory',
    start_date=datetime(2024, 1, 1),
    schedule_interval='10 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['fill_tables'],
) as dag:
    PythonOperator(
        task_id=upload_events.__name__,
        python_callable=upload_events_wrapper,
        op_args=[shared_dir_path],
        dag=dag,
    )
