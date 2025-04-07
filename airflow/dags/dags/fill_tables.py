import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
import os
from airflow.utils.dates import days_ago
import sys
from ..lib.generate_events import (
    generate_order_event,
    generate_product_events,
    generate_store_events,
    generate_sales_events
)

dag_name = os.path.basename(__file__).replace('.py', '')
default_args = {
    'start_date': datetime(2024, 1, 1),
}
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

with DAG(
        dag_name,
        default_args=default_args,
        description='fill tables with data from the .csv files in import_files directory',
        schedule='',
        catchup=False,
        max_active_runs=1,
        tags=[
            'initialization',
            'fill_tables'
        ]
) as dag:
    pass
#     run_fill_stores = PythonOperator(
#         task_id='fill_stores',
#         python_callable=run_fill_stores_procedure
#     )
#     run_fill_products = PythonOperator(
#         task_id='fill_products',
#         python_callable=run_fill_products_procedure
#     )
#     run_fill_orders = PythonOperator(
#         task_id='fill_orders',
#         python_callable=run_fill_orders_procedure
#     )
#     run_fill_sales = PythonOperator(
#         task_id='fill_sales',
#         python_callable=run_fill_sales_procedure
#     )
#     run_fill_stores >> run_fill_products >> run_fill_orders >> run_fill_sales
