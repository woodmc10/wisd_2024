import json
from google.cloud import storage
from build_dataframe import build_bat_path_df

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
from dtaidistance import dtw_visualisation as dtwvis
from dtaidistance import dtw

def filter_path(path_df):
    mid_index = path_df.index[path_df.event.isin(['Hit', 'Nearest'])].to_list()
    start_index = mid_index[0] - 75
    end_index = mid_index[0] + 75
    return path_df[start_index:end_index]

def normalize_path(path):
    pos_columns = ['head_pos_0', 'head_pos_1', 'head_pos_2',
                   'handle_pos_0', 'handle_pos_1', 'handle_pos_2']
    return path[pos_columns] - path[pos_columns].iloc[0]

def combine_coordinates(df):
    return df[['head_pos_0', 'head_pos_1', 'head_pos_2',
               'handle_pos_0', 'handle_pos_1', 'handle_pos_2'
              ]].values

def calculate_dtw_distance(df1, df2):
    """
    Calculates the DTW distance between two dataframes by computing the DTW distance for each coordinate separately.
    
    :param df1: First dataframe.
    :param df2: Second dataframe.
    :return: Aggregated DTW distance.
    """
    coordinates = ['head_pos_0', 'head_pos_1', 'head_pos_2',
                   'handle_pos_0', 'handle_pos_1', 'handle_pos_2']
    total_distance = 0.0
    
    for coordinate in coordinates:
        seq1 = df1[coordinate].values
        seq2 = df2[coordinate].values
        distance = dtw.distance(seq1, seq2)
        total_distance += distance
    
    return total_distance

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

def process_files_in_list(bucket_name, file_list=[]):
    """
    Processes all JSON files in a GCS bucket and builds a summary dictionary for each.

    :param bucket_name: Name of the GCS bucket.
    :param prefix: Prefix to filter files (optional).
    :return: List of summary dictionaries.
    """

    batter_id= 'None'
    batter_count = 0
    distance_metric_list = []
    for file_path in file_list:
        distance_metric_dict = dict()
        print(f"Processing file: {file_path}")
        json_file = read_json_from_gcs(bucket_name, file_path)
        file_batter_id = json_file['events'][0]['personId']['mlbId']
        bat_df = build_bat_path_df(json_file)
        if len(bat_df) < 75: 
            continue
        if bat_df.index[bat_df.event.isin(['Hit', 'Nearest'])].to_list()[0] > 74:
            filter_bat_df = filter_path(bat_df)
            normal_bat_df = normalize_path(filter_bat_df)
            if batter_id == file_batter_id:
                batter_count += 1
                if reference_bat_df.shape == normal_bat_df.shape:
                    distance = calculate_dtw_distance(reference_bat_df, normal_bat_df)
                else:
                    distance = -1.0
            else:
                batter_id = file_batter_id
                batter_count = 0
                reference_bat_df = normal_bat_df
                distance = 0.0
        else:
            distance = -2.0
        distance_metric_dict['file'] = file_path
        distance_metric_dict['batter'] = file_batter_id
        distance_metric_dict['batter_count'] = batter_count
        distance_metric_dict['distance'] = distance
        distance_metric_list.append(distance_metric_dict)
    return distance_metric_list

if __name__ == '__main__':

    bucket_name = '2024-hackathon'

    summary_df = pd.read_csv('summary_df.csv')
    batters_df = summary_df[
        (summary_df['batter'].notnull()) 
        &
        (summary_df['action'] != 'HitByPitch')
        &
        (summary_df['pitch_result'] != 'Ball')
            # I'll want to decide what to do with this situation,
            # for now dropping it will work
            # situation = check swing?
        ].sort_values('batter')
    file_list = batters_df.file_path.to_list()

    # Process all files in the GCS bucket and get summaries
    metrics = process_files_in_list(bucket_name, file_list)
    metrics_df = pd.DataFrame.from_dict(metrics)
    metrics_df.to_csv('distance_metrics_df.csv')

