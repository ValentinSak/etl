import json
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
from datetime import datetime
import csv
from repository import execute_statement_without_result, execute_batch_insert, execute_statement_as_dataframe
print(sys.path.append('/Users/valentinsak/PycharmProjects/etl/airflow_src'))


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


def run_fill_events_from_files_procedure(file_path: str, file_name: str):    
    query = '''
        CALL etl.load_csv_file(%s, %s);
    '''
    print(query)
    execute_statement_without_result(query, (file_path, file_name))


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

    with open(f'{file_path}/{batch_id}.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL, escapechar='\\')
        writer.writerow(headers)

        for event_type, payload in events:
            if isinstance(payload, str):
                try:
                    payload = json.dumps(json.loads(payload))
                except json.JSONDecodeError:
                    pass
            else:
                payload = json.dumps(payload)
            
            payload = payload.replace('\n', ' ')
            
            row = [batch_id, event_type, payload, batch_created_at]
            writer.writerow(row)

    print(f"Written {len(events)} events to {batch_id}")


def write_events_to_table(**context):
    values = [(event_type, json.dumps(payload), batch_id) for event_type, payload in events]

    insert_query = '''
        INSERT INTO etl.raw_events (event_type, payload, batch_id)
        VALUES %s
    '''

    execute_batch_insert(insert_query, values)


def get_all_processed_files() -> list[str]:
    query = '''
        SELECT
            filename
        FROM utilities.processed_files
    '''
    print(query)
    df = execute_statement_as_dataframe(query)
    processed_files = df['filename'].tolist()

    return processed_files

def upload_events(data_dir: str, processed_files: list[str]):
    for file in os.listdir(data_dir):
        if file.endswith('csv') and file not in processed_files:
            full_path = os.path.join(data_dir, file)
            try:
                run_fill_events_from_files_procedure(full_path, file)
                print(f'uploaded file {file}')
            except Exception as err:
                print(f"Error processing {file}: {err}")


if __name__ == '__main__':
    from dotenv import load_dotenv
    import os

    # load_dotenv()
    # events = generate_events()
    # print(events)
    # write_events_to_csv(events, '/Users/valentinsak/PycharmProjects/etl/shared_data_S3_replacement')
    # process_files = get_all_processed_files()
    # print(process_files)
    
    # query = '''
    #     CALL etl.load_csv_file(%s, %s);
    # '''
    # execute_statement_without_result(query, ('/shared/some_id_1.csv', 'some_id_1.csv'))

    # with open('/Users/valentinsak/PycharmProjects/etl/shared_data_S3_replacement/some_id_1.csv', 'r') as file:
    #     reader = csv.reader(file)
    #     headers = next(reader)  # Skip header
    #     for row in reader:
    #         print(row)

    # with open("/Users/valentinsak/PycharmProjects/etl/shared_data_S3_replacement/some_id_1.csv", "r", encoding="utf-8") as f:
    #     for i, line in enumerate(f, 1):
    #         print(f"{i}: {repr(line)}")