import pandas as pd
import json

def build_ball_flight_df(json_file):
    row_list = []
    for row in json_file['samples_ball']:
        row_dict = dict()
        row_dict['time'] = row.get('time', 'no_time')
        row_dict['pos_0'] = row.get('pos', ['no_pos', 'no_pos', 'no_pos'])[0]
        row_dict['pos_1'] = row.get('pos', ['no_pos', 'no_pos', 'no_pos'])[1]
        row_dict['pos_2'] = row.get('pos', ['no_pos', 'no_pos', 'no_pos'])[2]
        row_dict['vel_0'] = row.get('vel', ['no_vel', 'no_vel', 'no_vel'])[0]
        row_dict['vel_1'] = row.get('vel', ['no_vel', 'no_vel', 'no_vel'])[1]
        row_dict['vel_2'] = row.get('vel', ['no_vel', 'no_vel', 'no_vel'])[2]
        row_dict['acc_0'] = row.get('acc', ['no_acc', 'no_acc', 'no_acc'])[0]
        row_dict['acc_1'] = row.get('acc', ['no_acc', 'no_acc', 'no_acc'])[1]
        row_dict['acc_2'] = row.get('acc', ['no_acc', 'no_acc', 'no_acc'])[2]
        row_list.append(row_dict)
    return pd.DataFrame.from_dict(row_list)

def build_bat_path_df(json_file):
    row_list = []
    for row in json_file['samples_bat']:
        row_dict = dict()
        row_dict['event'] = row.get('event', 'no_event')
        row_dict['time'] = row.get('time', 'no_time')
        
        head = row.get('head', dict())
        row_dict['head_pos_0'] = head.get('pos', ['no_head_pos', 'no_head_pos', 'no_head_pos'])[0]
        row_dict['head_pos_1'] = head.get('pos', ['no_head_pos', 'no_head_pos', 'no_head_pos'])[1]
        row_dict['head_pos_2'] = head.get('pos', ['no_head_pos', 'no_head_pos', 'no_head_pos'])[2]

        handle = row.get('handle', dict())
        row_dict['handle_pos_0'] = handle.get('pos', ['no_handle_pos', 'no_handle_pos', 'no_handle_pos'])[0]
        row_dict['handle_pos_1'] = handle.get('pos', ['no_handle_pos', 'no_handle_pos', 'no_handle_pos'])[1]
        row_dict['handle_pos_2'] = handle.get('pos', ['no_handle_pos', 'no_handle_pos', 'no_handle_pos'])[2]
        row_list.append(row_dict)
    return pd.DataFrame.from_dict(row_list)

def build_summary_dict(json_file, file_path):
    summary_dict = dict()
    summary_dict['file_path'] = file_path
    summary_dict['pitch_id'] = json_file['summary_acts']['pitch']['eventId']
    summary_dict['pitch_type'] = json_file['summary_acts']['pitch']['type']
    summary_dict['pitch_result'] = json_file['summary_acts']['pitch']['result']
    summary_dict['action'] = json_file['summary_acts']['pitch']['action']
    summary_dict['hit_id'] = json_file['summary_acts']['hit']['eventId']
    summary_dict['stroke'] = json_file['summary_acts']['stroke']['type']
    summary_dict['event_len'] = len(json_file['events'])
    return summary_dict


#########
# Code from Chat GPT
import json
from google.cloud import storage

def read_json_from_gcs(bucket_name, file_path):
    """
    Reads a JSON file from a GCS bucket and returns the parsed JSON object.

    :param bucket_name: Name of the GCS bucket.
    :param file_path: Path to the file in the GCS bucket.
    :return: Parsed JSON object.
    """
    # Initialize a client
    client = storage.Client()

    # Get the bucket and blob
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Read the blob content as string and parse JSON
    json_data = blob.download_as_text()
    return json.loads(json_data)

# def build_summary_dict(json_file, file_path):
#     """
#     Builds a summary dictionary from the given JSON file and file path.

#     :param json_file: Parsed JSON object.
#     :param file_path: Path to the file.
#     :return: Dictionary with summary information.
#     """
#     summary_dict = dict()
#     summary_dict['file_path'] = file_path
#     summary_dict['pitch_id'] = json_file['summary_acts']['pitch']['eventId']
#     summary_dict['pitch_type'] = json_file['summary_acts']['pitch']['type']
#     summary_dict['pitch_result'] = json_file['summary_acts']['pitch']['result']
#     summary_dict['action'] = json_file['summary_acts']['pitch']['action']
#     summary_dict['hit_id'] = json_file['summary_acts']['hit']['eventId']
#     summary_dict['stroke'] = json_file['summary_acts']['stroke']['type']
#     summary_dict['event_len'] = len(json_file['events'])
#     return summary_dict

def process_all_files_in_bucket(bucket_name, prefix=''):
    """
    Processes all JSON files in a GCS bucket and builds a summary dictionary for each.

    :param bucket_name: Name of the GCS bucket.
    :param prefix: Prefix to filter files (optional).
    :return: List of summary dictionaries.
    """
    # Initialize a client
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # List all blobs in the bucket with the given prefix
    blobs = bucket.list_blobs(prefix=prefix)

    summaries = []
    for blob in blobs:
        if blob.name.endswith('.jsonl'):
            print(f"Processing file: {blob.name}")
            json_file = read_json_from_gcs(bucket_name, blob.name)
            summary = build_summary_dict(json_file, blob.name)
            summaries.append(summary)
    
    return summaries

if __name__ == '__main__': 
    # Usage
    bucket_name = '2024-hackathon'
    prefix = 'anonymized-files-wisd'  # Adjust if needed

    # Process all files in the GCS bucket and get summaries
    summaries = process_all_files_in_bucket(bucket_name, prefix)
    summary_df = pd.DataFrame.from_dict(summaries)
    summary_df.to_csv('summary_df.csv')
