import os
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from lib.service import get_recipients
from lib.alerts.alerts import store_sales_alert
from lib.configs import configs


dag_name = os.path.basename(__file__).replace('.py', '')
default_args = {
    'start_date': datetime(2024, 1, 1),
}

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

recipients = get_recipients([1, 2])
threshold = configs['thresholds'][store_sales_alert.__name__]

with DAG(
        dag_name,
        default_args=default_args,
        description='etl fill main tables from events table',
        start_date=datetime(2024, 1, 1),
        schedule_interval='0 9 * * *',
        catchup=False,
        max_active_runs=1,
        tags=[
            'alert',
            'store_sales'
        ]
) as dag:
    PythonOperator(
        task_id=store_sales_alert.__name__,
        python_callable=store_sales_alert,
        op_args=[recipients, threshold],
        dag=dag
    )

