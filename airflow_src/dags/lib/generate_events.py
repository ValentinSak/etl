import pandas as pd
from datetime import datetime, timedelta
import random
import string
import numpy as np
from service import execute_statement_as_dataframe
from psycopg2 import sql



def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))


def get_random_ids_from_csv(csv_file: str, column_name: str, n: int) -> list:
    df = pd.read_csv(csv_file, usecols=[column_name])
    return df[column_name].sample(n=n, replace=True).tolist()


def get_random_ids_from_df(df: pd.DataFrame, column_name: str, n: int) -> list:
    return df[column_name].sample(n=n, replace=True).tolist()


def get_values_from_column(schema_name: str, table_name: str, column_name: str):
    query = sql.SQL(
        '''
            SELECT
                {}
            FROM {}.{}
        '''
    ).format(
        sql.Identifier(column_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name)
    )
    df = execute_statement_as_dataframe(query)

    return df


def generate_order_event():
    return {
        'created_at': (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S"),
        'user_id': (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
    }


def generate_product_events():
    return {
        'created_at': (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S"),
        'name': generate_random_string(random.randint(5, 10))
    }


def generate_store_events():
    return {
        'created_at': (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S"),
        'name': generate_random_string(random.randint(5, 10)),
        'tax_id': random.randint(10 ** 11, 10 ** 12),
        'status': 'active'
    }


def generate_sales_events(quantity: int) -> list:
    order_ids_df = get_values_from_column('etl', 'orders', 'id')
    product_ids_df = get_values_from_column('etl', 'products', 'id')
    store_ids_df = get_values_from_column('etl', 'orders', 'id')
    unique_order_ids = get_random_ids_from_df(order_ids_df, "id", int(quantity * 0.6))
    product_ids = get_random_ids_from_df(product_ids_df, "id", quantity)
    store_ids = get_random_ids_from_df(store_ids_df, "id", quantity)
    quantities = np.random.randint(1, 11, size=quantity)

    sales_events = []
    current_index = 0

    for order_id in unique_order_ids:
        repeat_count = random.randint(1, 3)
        for _ in range(repeat_count):
            if current_index >= quantity:
                break
            event = {
                "sale_date": (datetime.now() - timedelta(minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S"),
                "order_id": order_id,
                "store_id": store_ids[current_index],
                "product_id": product_ids[current_index],
                "quantity": int(quantities[current_index])
            }
            sales_events.append(event)
            current_index += 1
    return sales_events

def generate_events():
    store_events = []
    for i in range(5):
        pass

