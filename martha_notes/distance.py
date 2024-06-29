from build_dataframe import build_bat_path_df, read_json_from_gcs

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

def process_files_in_list(bucket_name, file_list=[]):
    """
    Processes all JSON files in a GCS bucket and builds a summary dictionary for each.

    :param bucket_name: Name of the GCS bucket.
    :param prefix: Prefix to filter files (optional).
    :return: List of summary dictionaries.
    """

    batter_id = 'None'
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

def get_grade(distance, thresholds):
    # should this be moved to a utils file?
    if distance >= thresholds['A']:
        return 'A'
    elif distance > thresholds['B']:
        return 'B'
    elif distance > thresholds['C']:
        return 'C'
    elif distance > thresholds['D']:
        return 'D'
    else:
        return 'F'
    
def convert_column_name(column_name):
    # should this be moved to a utils file?
    bat_section = column_name.split('_')[0].capitalize()
    axes_list = ['X', 'Y', 'Z']
    letter = axes_list[int(column_name.split('_')[-1])]
    return f'{bat_section} {letter}'

def color_letter(grade):
    # should this be moved to a utils file?
    color_dict = {
        'A': 'green',
        'B': 'blue',
        'C': 'orange',
        'D': 'red',
        'F': 'red'
    }
    return color_dict[grade]

def vis_distance(df_1, df_2, pos, grade):
    seq1 = df_1[pos].values
    seq2 = df_2[pos].values
    alignment_path = dtw.warping_path(seq1, seq2)
    
    plt.figure(figsize=(12, 6))
    plt.plot(seq1, label=f'Swing 1 {convert_column_name(pos)}')
    plt.plot(seq2, label=f'Swing 2 {convert_column_name(pos)}')
    for (i, j) in alignment_path:
        plt.plot([i, j], [seq1[i], seq2[j]], 'k-', alpha=0.5)
    plt.text(5, 2, grade, fontsize=100, color=color_letter(grade))

    plt.legend()
    plt.show()

def distance_scorecard(distance_df): 
    batters = distance_df.batter.unique()
    scorecard_list = []
    for batter in batters:
        scorecard_dict = dict()
        scorecard_dict['batter'] = batter
        batter_df = distance_df[(distance_df['batter'] == batter) & (distance_df['distance'] >= 0)]
            # distance values of -2 and -1 indicate specific data situations, and should not
            # be included in the variance calculations
        if len(batter_df) == 0:
            continue
        min_dist = np.mean(batter_df['distance']) - 2 * np.std(batter_df['distance'])
        max_dist = np.mean(batter_df['distance']) + 2 * np.std(batter_df['distance'])
        swing_count = len(batter_df)
        good_count = len(batter_df[
                        (batter_df['distance'] < max_dist)
                        &
                        (batter_df['distance'] > min_dist)
                        ])
        scorecard_dict['swing_count'] = swing_count
        scorecard_dict['dist_score'] = good_count/swing_count
        distance_thresholds = {'A': 1, 'B': 0.95, 'C': 0.9, 'D': 0.85}
        scorecard_dict['dist_grade'] = get_grade(good_count/swing_count, distance_thresholds)
        scorecard_dict['reference_file'] = batter_df[batter_df['distance'] == 0.0]['file'].values[0]
        scorecard_dict['compare_file'] = batter_df[batter_df['distance'] == max(batter_df['distance'])]['file'].values[0]
        scorecard_list.append(scorecard_dict)
    return scorecard_list

if __name__ == '__main__':

    bucket_name = '2024-hackathon'

    # summary_df = pd.read_csv('summary_df.csv')
    # batters_df = summary_df[
    #     (summary_df['batter'].notnull()) 
    #     &
    #     (summary_df['action'] != 'HitByPitch')
    #     &
    #     (summary_df['pitch_result'] != 'Ball')
    #         # I'll want to decide what to do with this situation,
    #         # for now dropping it will work
    #         # situation = check swing?
    #     ].sort_values('batter')
    # file_list = batters_df.file_path.to_list()

    # # Process all files in the GCS bucket and get summaries
    # metrics = process_files_in_list(bucket_name, file_list)
    # metrics_df = pd.DataFrame.from_dict(metrics)
    # metrics_df.to_csv('distance_metrics_df.csv')

    distance_metrics_df = pd.read_csv('distance_metrics_df.csv')
    scorecard_df = pd.DataFrame.from_dict(distance_scorecard(distance_metrics_df))
    batter_list = [849653732]
        # when ready, create a list of all batters in scorecard_df and loop through
            # need to add code to save the resulting plots
    for batter_id in batter_list:
        reference_file = scorecard_df[scorecard_df['batter'] == batter_id]['reference_file'].values[0]
        print(reference_file)
        reference_json = read_json_from_gcs(bucket_name, reference_file)
        reference_df = normalize_path(filter_path(build_bat_path_df(reference_json)))
        compare_file = scorecard_df[scorecard_df['batter'] == batter_id]['compare_file'].values[0]
        compare_json = read_json_from_gcs(bucket_name, compare_file)
        compare_df = normalize_path(filter_path(build_bat_path_df(compare_json)))
        grade = scorecard_df[scorecard_df['batter'] == batter_id]['dist_grade'].values[0]
        vis_distance(reference_df, compare_df, 'head_pos_0', grade)

"""
Similarity score is an evaluation of how consistent a player is with each of their swings. The path of
each swing is compared to a reference swing (the first swing by a batter in the dataset). The similarity
of each swing is calculated using dynamic time warping (DTW). DTW is useful for comparing time series
data because it allows for stretching and compressing of the time component when determining the similarity
of the path. Swings were normalized by taking 75 frames before contact and 75 frames after contact, and by
subtracting each frame from the starting frame. This provided a baseline swing location so similarity
was not impacted by the batter changing their location in the box. The similarity of each component of the
swing was evaluated separately and combined to generate a single similarity measure for each swing. The
swing similarity grade is determined by finding the percent of swings that are outliers from the batters
similarity scores sample.

The visualization provides the batter's grade and a comparison of the batter's reference swing and their
least similar swing.

Possible adjustments
- different score calculation: variance may not be the right way to give a consistency score
- sword detection and visualization
"""