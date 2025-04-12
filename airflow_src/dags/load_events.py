from pathlib import Path
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print(os.path.dirname(os.path.abspath(__file__)))
root = str(Path(__file__).resolve().parent.parent)
print(root)
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from lib.service import write_events_to_csv
from lib.generate_events import generate_events
from lib.configs import configs

dag_name = os.path.basename(__file__).replace('.py', '')
default_args = {
    'start_date': datetime(2024, 1, 1),
}
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

events = generate_events()
shared_dir_path = configs['shared_dir_path']

with DAG(
        dag_name,
        default_args=default_args,
        description='write events to csv file in shared directory',
        start_date=datetime(2024, 1, 1),
        schedule_interval='0 12,20 * * *',
        catchup=False,
        max_active_runs=1,
        tags=[
            'fill_tables'
        ]
) as dag:
    PythonOperator(
        task_id='write_events_to_csv',
        python_callable=write_events_to_csv,
        op_args=[events, shared_dir_path],
        provide_context=True, 
        dag=dag
    )
