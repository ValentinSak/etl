import pandas as pd
from datetime import datetime, timedelta
import random
import string
import numpy as np
from repository import execute_statement_as_dataframe
from psycopg2 import sql
import json


def generate_random_string(length: int) -> str:
    '''Generates a random string of specified length using ASCII letters
    params:
    length - length of the string to generate
    '''
    return ''.join(random.choices(string.ascii_letters, k=length))


def get_random_ids_from_df(df: pd.DataFrame, column_name: str, n: int) -> list:
    '''Gets n random ids from a specified column in a DataFrame

    params:
    df - DataFrame containing the ids
    column_name - name of the column containing ids
    n - number of random ids to select
    '''
    return df[column_name].sample(n=n, replace=True).tolist()


def get_values_from_db_column(
    schema_name: str, table_name: str, column_name: str
) -> pd.DataFrame:
    '''Retrieves all values from a specified column in a database table

    params:
    schema_name - name of the database schema
    table_name - name of the table
    column_name - name of the column to retrieve values from
    '''
    query = sql.SQL(
        '''
            SELECT
                {}
            FROM {}.{}
        '''
    ).format(
        sql.Identifier(column_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    df = execute_statement_as_dataframe(query)

    return df


def generate_order_event() -> str:
    '''Generates a random order event with timestamp and user id

    Returns a JSON string containing:
    - created_at: current timestamp with random minutes offset
    - user_id: random integer between 1 and 10001
    '''
    event_data = {
        'created_at': (
            datetime.now() - timedelta(minutes=random.randint(0, 59))
        ).strftime('%Y-%m-%d %H:%M:%S'),
        'user_id': random.randint(1, 10001),
    }

    return json.dumps(event_data)


def generate_product_event() -> str:
    '''Generates a random product event with timestamp, name, and price

    Returns a JSON string containing:
    - created_at: current timestamp with random minutes offset
    - name: random string of length 5-10
    - price: random float between 1 and 500
    '''
    event_data = {
        'created_at': (
            datetime.now() - timedelta(minutes=random.randint(0, 59))
        ).strftime('%Y-%m-%d %H:%M:%S'),
        'name': generate_random_string(random.randint(5, 10)),
        'price': round(random.uniform(1, 500), 2),
    }

    return json.dumps(event_data)


def generate_store_event() -> str:
    '''Generates a random store event with timestamp, name, tax ID, and status

    Returns a JSON string containing:
    - created_at: current timestamp with random minutes offset
    - name: random string of length 5-10
    - tax_id: random 11-12 digit number
    - status: always set to 'active'
    '''
    event_data = {
        'created_at': (
            datetime.now() - timedelta(minutes=random.randint(0, 59))
        ).strftime('%Y-%m-%d %H:%M:%S'),
        'name': generate_random_string(random.randint(5, 10)),
        'tax_id': random.randint(10**11, 10**12),
        'status': 'active',
    }

    return json.dumps(event_data)


def generate_sales_events(quantity: int) -> list[tuple[str, str]]:
    '''Generates a list of sales events with random data

    params:
    quantity - number of sales events to generate

    Returns a list of tuples where each tuple contains:
    - event type ('sales_event')
    - JSON string with sales data (sale_date, order_id, store_id, product_id, quantity)
    '''
    order_ids_df = get_values_from_db_column('etl', 'orders', 'id')
    product_ids_df = get_values_from_db_column('etl', 'products', 'id')
    store_ids_df = get_values_from_db_column('etl', 'stores', 'id')
    unique_order_ids = get_random_ids_from_df(order_ids_df, 'id', int(quantity * 0.6))
    product_ids = get_random_ids_from_df(product_ids_df, 'id', quantity)
    store_ids = get_random_ids_from_df(store_ids_df, 'id', quantity)
    quantities = np.random.randint(1, 11, size=quantity)
    sales_events = []
    current_index = 0

    for order_id in unique_order_ids:
        repeat_count = random.randint(1, 3)
        for _ in range(repeat_count):
            if current_index >= quantity:
                break
            event = {
                'sale_date': (
                    datetime.now() - timedelta(minutes=random.randint(0, 59))
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'order_id': order_id,
                'store_id': store_ids[current_index],
                'product_id': product_ids[current_index],
                'quantity': int(quantities[current_index]),
            }
            sales_events.append(('sales_event', json.dumps(event)))
            current_index += 1

    return sales_events


def generate_events() -> list[tuple[str, str]]:
    '''Generates a mix of different types of events

    Returns a list of tuples containing:
    - 2 store events
    - 2 product events
    - 10 order events
    - 50 sales events
    Each tuple contains event type and JSON string with event data
    '''
    store_events = [('store_event', generate_store_event()) for i in range(2)]
    product_events = [('product_event', generate_product_event()) for i in range(2)]
    order_events = [('order_event', generate_order_event()) for i in range(10)]
    sales_events = generate_sales_events(50)

    events = store_events + product_events + order_events + sales_events

    return events
