import pandas as pd
from service import execute_statement_without_result
from sql_scripts import create_schema
from datetime import datetime
from airflow import DAG
import os

dag_name = os.path.basename(__file__).replace('.py', '')
default_args = {}
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

# with DAG(
#     dag_name,
#     default_args=default_args,
#     description='create basic schema and tables',
#     schedule_interval=None,
#     catchup=False,
#     max_active_runs=1,
#     tags=[
#         'initialization',
#     ]
# ) as dag:
#     create_schema =