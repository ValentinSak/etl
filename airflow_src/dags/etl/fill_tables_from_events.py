import os
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from lib.repository import execute_statement_without_result


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
        description='etl fill main tables from events table',
        start_date=datetime(2024, 1, 1),
        schedule_interval='10 * * * *',
        catchup=False,
        max_active_runs=1,
        tags=[
            'fill_tables'
        ]
) as dag:
    with TaskGroup("fill_tables_from_events") as fill_group:
        fill_stores = PythonOperator(
            task_id='fill_stores',
            python_callable=execute_statement_without_result,
            op_args=['CALL etl.fill_stores_from_events();'],
            dag=dag
        )

        fill_products = PythonOperator(
            task_id='fill_products',
            python_callable=execute_statement_without_result,
            op_args=['CALL etl.fill_products_from_events();'],
            dag=dag
        )

        fill_orders = PythonOperator(
            task_id='fill_orders',
            python_callable=execute_statement_without_result,
            op_args=['CALL etl.fill_orders_from_events();'],
            dag=dag
        )

        fill_sales = PythonOperator(
            task_id='fill_sales',
            python_callable=execute_statement_without_result,
            op_args=['CALL etl.fill_sales_from_events();'],
            dag=dag
        )

        fill_stores >> fill_products >> fill_orders >> fill_sales

