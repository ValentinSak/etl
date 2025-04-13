import psycopg2
import os
import pandas as pd
from psycopg2 import sql
from pathlib import Path
# from dotenv import load_dotenv
from psycopg2.extras import execute_values

# dotenv_path = Path(__file__).parent / ".env"
# load_dotenv(dotenv_path)


def get_etl_connection():
    conn_string = f'dbname={os.getenv("ETL_DB_NAME")} ' \
                  f'host={os.getenv("ETL_DB_HOST")} ' \
                  f'port={os.getenv("ETL_DB_PORT")} ' \
                  f'user={os.getenv("ETL_DB_USER")} ' \
                  f'password={os.getenv("ETL_DB_PASSWORD")}'
    print(conn_string)
    connection = psycopg2.connect(conn_string)

    return connection


def execute_statement_without_result(query: str, params=None) -> None:
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            print(f'number of rows affected: {cursor.rowcount}')
            conn.commit()


def execute_batch_insert(query: str, values: list):
    with get_etl_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
            print(f'inserted {cur.rowcount} rows')
        conn.commit()


def execute_statement_as_dataframe(query, params=None) -> pd.DataFrame:
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            if isinstance(query, sql.SQL):
                query = query.as_string(conn)
            cursor.execute(query, params)
            columns = [i[0] for i in cursor.description]
            result = cursor.fetchall()

            return pd.DataFrame(result, columns=columns)
