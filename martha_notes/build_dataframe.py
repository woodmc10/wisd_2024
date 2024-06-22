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