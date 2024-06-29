import pandas as pd
from build_dataframe import build_bat_path_df, read_json_from_gcs
from distance import get_grade

def process_files_in_list(bucket_name, file_list=[]):
    batter_id = 'None'
    batter_count = 0
    timing_metric_list = []
    for file_path in file_list:
        timing_metric_dict = dict()
        print(f"Processing file: {file_path}")
        json_file = read_json_from_gcs(bucket_name, file_path)
        file_batter_id = json_file['events'][0]['personId']['mlbId']
        bat_df = build_bat_path_df(json_file)
        hit_frame = bat_df[bat_df['event'].isin(['Hit', 'Nearest'])]
        if len(hit_frame) == 0:
            continue
        handle_loc = hit_frame['handle_pos_1'].values[0]
        head_loc = hit_frame['head_pos_1'].values[0]
        contact_loc = (handle_loc + head_loc) / 2
        if batter_id == file_batter_id:
            batter_count += 1
        else:
            batter_id = file_batter_id
            batter_count = 0
        timing_metric_dict['file'] = file_path
        timing_metric_dict['batter'] = file_batter_id
        timing_metric_dict['batter_count'] = batter_count
        timing_metric_dict['contact_y_loc'] = contact_loc
        timing_metric_list.append(timing_metric_dict)
    return timing_metric_list

def score_contact_loc(contact_loc):
    quality_locations = [18/12, 10/12, 2/12, -6/12]
    if contact_loc > quality_locations[0]:
        score = 0
    elif contact_loc > quality_locations[1]:
        score = 4
    elif contact_loc > quality_locations[2]:
        score = 3
    elif contact_loc > quality_locations[3]:
        score = 2
    else:
        score = 0
    return score

def timing_scorecard(timing_df):
    scorecard_list = []
    for batter in timing_df['batter'].unique():
        scorecard_dict = dict()
        scorecard_dict['batter'] = batter

        batter_df = timing_df[
            (timing_df['batter'] == batter)
            &
            (timing_df['contact_y_loc'] != 0.0)
        ]
        swing_count = len(batter_df)
        scorecard_dict['swing_count'] = swing_count
        batter_df['score'] = batter_df['contact_y_loc'].apply(score_contact_loc)
        contact_score_total = sum(batter_df['score'])
        contact_score_avg = contact_score_total / swing_count
        scorecard_dict['timing_avg'] = contact_score_avg
        timing_thresholds = {'A': 4, 'B': 3, 'C': 2, 'D': 1}
        scorecard_dict['timing_grade'] = get_grade(contact_score_avg, timing_thresholds)
        scorecard_list.append(scorecard_dict)
    return scorecard_list


if __name__ == '__main__':
    
    # bucket_name = '2024-hackathon'
    
    # # Find list of files to process from summary csv
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
    # metrics_df.to_csv('timing_metrics_df.csv')

    timing_metrics_df = pd.read_csv('timing_metrics_df.csv')
    timing_score_list = timing_scorecard(timing_metrics_df)
    timing_score_df = pd.DataFrame.from_dict(timing_score_list)
    print(timing_score_df)

"""
Quality contact requires excellent timing during the at-bat. This approach to scoring the batter's
timing only considers the contact location, which leaves out important compoenents such as pitch
location and contact quality. The contact location is determined by finding the average of the 
head_pos_1 and handle_pos_1 value in the row labeled as "Hit" or "Nearest" in the bat tracking
data. Three section around the plate were assigned score values and each at bat recieved a score.
The final score for each batter is the sum of all timing scores divided by the number of swings
given a score.
"""