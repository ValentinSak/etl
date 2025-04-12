import json
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime
import csv
from repository import execute_statement_without_result, execute_batch_insert
dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)


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


def write_events_to_csv(events, file_path, **context):
    batch_id = context['run_id']
    headers = ['batch_id', 'event_type', 'payload', 'batch_created_at']
    batch_created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    events = events + []

    with open(f'{file_path}/{batch_id}.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if file.tell() == 0:
            writer.writerow(headers)

        for row in events:
            row_with_created_at = [batch_id] + row + [batch_created_at]
            writer.writerow(row_with_created_at)

    print(f"Written {len(events)} events to {batch_id}")


def write_events_to_table(**context):
    values = [(event_type, json.dumps(payload), batch_id) for event_type, payload in events]

    insert_query = '''
        INSERT INTO etl.raw_events (event_type, payload, batch_id)
        VALUES %s
    '''

    execute_batch_insert(insert_query, values)



# from dotenv import load_dotenv
# import os
#
# load_dotenv()
# write_events_to_table()