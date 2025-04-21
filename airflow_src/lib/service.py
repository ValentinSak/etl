import json
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime
import csv
from configs import configs
from repository import execute_statement_without_result, execute_statement_as_dataframe


dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)


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

def get_recipient(id: int) -> dict:
    try:
        recipient_config = [recipient for recipient in configs['recipients'] if recipient['id'] == id]
        if len(recipient_config) > 1:
            raise ValueError(f'there is more then one recipient with such id {id}')
        
        return recipient_config[0]
    
    except Exception as err:
        print(err)

def get_recipients(ids: list[int]) -> dict:
    try:
        recipient_configs = [recipient for recipient in configs['recipients'] if recipient['id'] in ids]
        
        return recipient_configs
    
    except Exception as err:
        print(err)