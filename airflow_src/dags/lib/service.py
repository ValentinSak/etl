from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import random
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path
from psycopg2 import sql


dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)


def get_etl_connection():
    conn_string = f'dbname={os.getenv("ETL_DB_NAME")} ' \
                  f'host={os.getenv("ETL_DB_HOST")} ' \
                  f'port={os.getenv("ETL_DB_PORT")} ' \
                  f'user={os.getenv("ETL_DB_USER")} ' \
                  f'password={os.getenv("ETL_DB_PASSWORD")}'
    connection = psycopg2.connect(conn_string)

    return connection


def execute_statement_without_result(query: str, params=None) -> None:
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            print(f'number of rows affected: {cursor.rowcount}')
            conn.commit()


def execute_statement_as_dataframe(query, params=None) -> pd.DataFrame:
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            if isinstance(query, sql.SQL):
                query = query.as_string(conn)
            cursor.execute(query, params)
            print(query.as_string(conn))
            columns = [i[0] for i in cursor.description]
            result = cursor.fetchall()

            return pd.DataFrame(result, columns=columns)


def copy_file_to_psql(schema_name: str, table_name: str, file_name: str) -> None:
    # root_path = find_project_root()
    # print(root_path)
    # file_path = os.path.join(root_path, f'airflow_src/dags/data/{file_name}')
    file_path = f'/data/{file_name}'
    print(file_path)
    # add check if schema or table or file exists
    query = f'''
        COPY {schema_name}.{table_name}
        FROM '{file_path}'
        WITH CSV HEADER;
    '''
    print(query)
    execute_statement_without_result(query)


def find_project_root(starting_path=None):
    current_path = Path(starting_path or Path(__file__).resolve())

    while current_path.parent != current_path:
        if (current_path / 'README.md').exists():
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError("Could not find the project root.")


def run_fill_stores_procedure():
    query = 'CALL etl.fill_stores();'
    print(query)
    execute_statement_without_result(query)


def run_fill_sales_procedure():
    query = 'CALL etl.fill_sales();'
    print(query)
    execute_statement_without_result(query)


def run_fill_orders_procedure():
    query = 'CALL etl.fill_orders();'
    print(query)
    execute_statement_without_result(query)


def run_fill_products_procedure():
    query = 'CALL etl.fill_products();'
    print(query)
    execute_statement_without_result(query)


def get_last_row_dict(csv_file: str) -> dict:
    headers = get_csv_headers(csv_file)
    with open(csv_file, "rb") as file:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b'\n':
            file.seek(-2, os.SEEK_CUR)
        last_row = file.readline().decode().strip()
    last_row_values = last_row.split(',')
    last_row_dict = dict(zip(headers, last_row_values))

    return last_row_dict


def get_csv_headers(csv_file: str) -> list:
    with open(csv_file, "r", encoding="utf-8") as file:
        header = file.readline().strip().split(",")

    return header


def get_ids_from_table():
    pass