from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path

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


def execute_statement_without_result(sql: str) -> None:
    conn = get_etl_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    print(f'number of rows affected: {cursor.rowcount}')
    conn.commit()
    cursor.close()
    conn.close()


def execute_statement_as_dataframe(sql) -> pd.DataFrame:
    conn = get_etl_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    result: pd.DataFrame = cursor.fetch_dataframe()
    cursor.close()
    conn.close()

    return result


def copy_file_to_psql(schema_name: str, table_name: str, file_name: str) -> None:
    # root_path = find_project_root()
    # print(root_path)
    # file_path = os.path.join(root_path, f'airflow/dags/data/{file_name}')
    file_path = f'/data/{file_name}'
    print(file_path)
    # add check if schema or table or file exists
    sql = f'''
        COPY {schema_name}.{table_name}
        FROM '{file_path}'
        WITH CSV HEADER;
    '''
    print(sql)
    execute_statement_without_result(sql)


def find_project_root(starting_path=None):
    current_path = Path(starting_path or Path(__file__).resolve())

    while current_path.parent != current_path:
        if (current_path / 'README.md').exists():
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError("Could not find the project root.")


if __name__ == '__main__':
    copy_file_to_psql('etl', 'stores', 'stores.csv')
    # conn = get_etl_connection()
    # print("Connection successful:", conn)
