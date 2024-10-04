import requests
import json
import csv
import pandas as pd
from dotenv import load_dotenv
import os
import time 

# Load environment variables from .env file
load_dotenv()

SKYFLOW_ACCOUNT_ID = os.getenv('SKYFLOW_ACCOUNT_ID')
BEARER_TOKEN = os.getenv('BEARER_TOKEN')
VAULT_ID = os.getenv('VAULT_ID')
URL = os.getenv('URL')

def deidentify_text(text):
    url = f'{URL}/v1/detect/text'
    
    headers = {
        'X-SKYFLOW-ACCOUNT-ID': SKYFLOW_ACCOUNT_ID,
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {BEARER_TOKEN}',
    }
    payload = {
        "text": text,
        "deidentify_option": "ENTITY_UNQ_COUNTER",
        "store_entities": False,
        "restrict_entity_types": ["name_given", "name_family", "name", "location_address_street", "location_city", "location_country", "credit_card", "email_address", "gender_sexuality", "numerical_pii", "ip_address", "occupation", "origin", "phone_number", "gender_sexuality", "url", "location_state", "organization", "location_zip", "username"],
        "restrict_regex":["\\b[a-zA-Z]+\.[a-zA-Z]+\\b"],
        "accuracy": "high_multilingual",
        "vault_id": VAULT_ID,
        "session_id": "1"
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Status Code: {response.status_code}')
        print('Headers:', response.headers)
        print('Response Body:', response.text)
        return {"error": response.status_code, "message": response.text}

def process_csv(input_csv):
    # Load CSV file into DataFrame
    df = pd.read_csv(input_csv)
    
    # Assume the second column is what we want to process
    column_to_process = df.columns[1]

    # New columns for processed text and entities
    df['processed_text'] = ''
    df['entities'] = ''

    # Process each row
    for index, row in df.iterrows():
        time.sleep(1)
        response = deidentify_text(row[column_to_process])
        if 'error' not in response:
            df.at[index, 'processed_text'] = response.get('processed_text', '')
            df.at[index, 'entities'] = json.dumps(response.get('entities', []), ensure_ascii=False)
        else:
            df.at[index, 'processed_text'] = response['message']  # Store error message in processed_text

    # Return processed data and fieldnames for CSV writing
    return df, df.columns.tolist()


def write_to_csv(data, fieldnames, output_csv):
    # If data is a DataFrame, write directly to CSV
    if isinstance(data, pd.DataFrame):
       data.to_csv(output_csv, index=False, columns=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    else:
        # Handle other cases if necessary
        pass


if __name__ == "__main__":
    input_csv_file = "INPUT_CSV_PATH"

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Process the CSV file
    processed_data, fieldnames = process_csv(input_csv_file)
    
    # Define output file paths
    output_csv_file = os.path.join(output_dir, "OUTPUT_CSV_PATH")

    # Write processed data to CSV
    write_to_csv(processed_data, fieldnames,output_csv_file)


    