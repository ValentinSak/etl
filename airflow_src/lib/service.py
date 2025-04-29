import json
import os
from datetime import datetime
import csv
from configs import configs
from repository import execute_statement_without_result, execute_statement_as_dataframe


def run_fill_events_from_files_procedure(file_path: str, file_name: str) -> None:
    """Executes a stored procedure to load events from a CSV file into the database
    
    params:
    file_path - full path to the CSV file
    file_name - name of the CSV file
    
    Calls the etl.load_csv_file stored procedure with the provided parameters
    """
    query = '''
        CALL etl.load_csv_file(%s, %s);
    '''
    print(query)
    execute_statement_without_result(query, (file_path, file_name))


def get_last_row_dict(csv_file: str) -> dict:
    """Retrieves the last row from a CSV file as a dictionary
    
    params:
    csv_file - path to the CSV file
    
    Returns a dictionary where keys are column headers and values are from the last row
    """
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
    """Gets the column headers from a CSV file
    
    params:
    csv_file - path to the CSV file
    
    Returns a list of column headers
    """
    with open(csv_file, "r", encoding="utf-8") as file:
        header = file.readline().strip().split(",")

    return header


def write_events_to_csv(events: list[tuple[str, str]], file_path: str, **context) -> None:
    """Writes events to a CSV file with batch information
    
    params:
    events - list of tuples containing (event_type, payload)
    file_path - directory path where the CSV file will be created
    context - dictionary containing Airflow context (must include 'run_id')
    
    Creates a CSV file with the following columns:
    - batch_id: from context['run_id']
    - event_type: type of the event
    - payload: JSON string of event data
    - batch_created_at: current timestamp
    """
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
    """Retrieves list of all processed files from the database
    
    Returns a list of filenames that have been processed
    """
    query = '''
        SELECT
            filename
        FROM utilities.processed_files
    '''
    print(query)
    df = execute_statement_as_dataframe(query)
    processed_files = df['filename'].tolist()

    return processed_files


def upload_events(data_dir: str, processed_files: list[str]) -> None:
    """Uploads unprocessed CSV files from a directory to the database
    
    params:
    data_dir - directory containing CSV files to process
    processed_files - list of filenames that have already been processed
    
    For each unprocessed CSV file:
    1. Attempts to load it into the database
    2. Prints success or error message
    """
    for file in os.listdir(data_dir):
        if file.endswith('csv') and file not in processed_files:
            full_path = os.path.join(data_dir, file)
            try:
                run_fill_events_from_files_procedure(full_path, file)
                print(f'uploaded file {file}')
            except Exception as err:
                print(f"Error processing {file}: {err}")


def get_recipient(id: int) -> dict:
    """Retrieves recipient configuration by ID
    
    params:
    id - recipient identifier
    
    Returns a dictionary containing recipient configuration
    
    Raises ValueError if multiple recipients with the same ID are found
    """
    try:
        recipient_config = [recipient for recipient in configs['recipients'] if recipient['id'] == id]
        if len(recipient_config) > 1:
            raise ValueError(f'there is more then one recipient with such id {id}')
        
        return recipient_config[0]
    
    except Exception as err:
        print(err)


def get_recipients(ids: list[int]) -> dict:
    """Retrieves multiple recipient configurations by their IDs
    
    params:
    ids - list of recipient identifiers
    
    Returns a dictionary containing configurations for all requested recipients
    """
    try:
        recipient_configs = [recipient for recipient in configs['recipients'] if recipient['id'] in ids]
        return recipient_configs
    except Exception as err:
        print(err)
