import psycopg2
import os
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import connection
from typing import Any


def get_etl_connection() -> connection:
    '''Creates and returns a connection to the ETL database'''
    conn_string = (
        f'dbname={os.getenv('ETL_DB_NAME')} '
        f'host={os.getenv('ETL_DB_HOST')} '
        f'port={os.getenv('ETL_DB_PORT')} '
        f'user={os.getenv('ETL_DB_USER')} '
        f'password={os.getenv('ETL_DB_PASSWORD')}'
    )
    print(conn_string)
    conn = psycopg2.connect(conn_string)

    return conn


def execute_statement_without_result(query: str, params: Any | None = None) -> None:
    '''Executes a SQL statement that doesn't return results

    params:
    query - SQL query to execute
    params - optional parameters for the query

    Commits the transaction after execution
    '''
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            print(f'number of rows affected: {cursor.rowcount}')
            conn.commit()


def execute_batch_insert(query: str, values: list) -> None:
    '''Executes a batch insert operation using execute_values

    params:
    query - SQL INSERT statement
    values - list of tuples containing values to insert

    Commits the transaction after execution
    '''
    with get_etl_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
            print(f'inserted {cur.rowcount} rows')
        conn.commit()


def execute_statement_as_dataframe(
    query: str | sql.SQL, params: Any | None = None
) -> pd.DataFrame:
    '''Executes a SQL query and returns results as a pandas DataFrame

    params:
    query - SQL query to execute (can be string or SQL object)
    params - optional parameters for the query

    Returns a DataFrame with column names from the query result
    '''
    with get_etl_connection() as conn:
        with conn.cursor() as cursor:
            if isinstance(query, sql.SQL):
                query = query.as_string(conn)
            cursor.execute(query, params)
            columns = [i[0] for i in cursor.description]
            result = cursor.fetchall()

            return pd.DataFrame(result, columns=columns)
