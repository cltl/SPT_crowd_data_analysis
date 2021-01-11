import pandas as pd
import crowdtruth
from crowdtruth.configuration import DefaultConfig
import os

from utils_data import load_experiment_data

import pandas as pd
import os
from collections import defaultdict

from datetime import datetime, time, timedelta
from utils_analysis import sort_by_key


def add_time_info_worker_batch(data_dict_list):

    times = []
    timestamp_data_dict = dict()
    for d in data_dict_list:
        timestamp = d['timestamp']
        f = '%d-%b-%Y %H:%M:%S'
        datetime_obj = datetime.strptime(timestamp, f)
        times.append(datetime_obj)
        timestamp_data_dict[str(datetime_obj)] = d
    times_sequence = sorted(times)
    durations = []
    for n, t in enumerate(times_sequence):
        if n != 0:
            previous_t = times_sequence[n-1]
            diff = t - previous_t
            durations.append(diff)

    if len(durations) > 1:
        mean_diff = sum(durations, timedelta(0))/len(durations)
    else:
        mean_diff = timedelta(
                 days=0,
                 seconds=7,
                 microseconds=0,
                 milliseconds=0,
                 minutes=0,
                 hours=0,
                 weeks=0)

    beginning = times_sequence[0] - mean_diff

    for n, t in enumerate(times_sequence):
        d = timestamp_data_dict[str(t)]
        if n != 0:
            start = times_sequence[n-1]
            end = t
        else:
            start = beginning
            end = t
        d['_started_at'] = str(start)
        d['_created_at'] = str(end)

def add_time_info(data_dict_list):
    data_by_batch = sort_by_key(data_dict_list, ['completionurl'])
    for batch, b_data in data_by_batch.items():
        data_by_worker = sort_by_key(b_data, ['workerid'])
        for w, w_data in data_by_worker.items():
            add_time_info_worker_batch(w_data)



def create_input_df(all_question_dicts):
    """
    Create final dataframe in format expected by crowdtruth (figure8)

    :param list all_question_dicts: List of all annotated units with time info
    :return: dataframe in expected format
    """
    final_df = pd.DataFrame.from_records(all_question_dicts)
    # make name changes so the format will be recognized
    final_df.rename(columns={'quid': '_unit_id'}, inplace = True)
    final_df.rename(columns={'id': '_id'}, inplace = True)
    final_df.rename(columns={'workerid': '_worker_id'}, inplace = True)
    headers_to_drop =['assignmentid',
                        'completionurl', 'exampletrue', 'examplefalse', 'filename', 'hitid',
                        'listnumber', 'origin', 'partid', 'questionid', 'run', 'sublist', 'timestamp']
    headers_to_drop_in_df = []
    for h in headers_to_drop:
        if h in final_df.columns:
            headers_to_drop_in_df.append(h)
    final_df.drop(columns = headers_to_drop_in_df, inplace = True)
    #checks = ['check1', 'check2', 'check3', 'check4']
    #for ch in checks:
    #    final_df.drop(final_df[final_df['_unit_id'] == ch].index, inplace = True)
    # get colums in correct order
    col_order = ['_unit_id', '_id', '_worker_id', '_started_at',
                 '_created_at', 'relation', 'concept',
                 'property', 'answer',]
    final_df = final_df[col_order]
    return final_df

class TestConfig(DefaultConfig):
    inputColums = ['relation',  'property', 'concept']
    outputColumns = ['answer']
    annotation_separator = ','
    open_ended_task = False
    annotation_vector = ['true', 'false']

    def processJudgments(self, judgments):
        # pre-process output to match the values in annotation_vector
        for col in self.outputColumns:
            # transform to lowercase
            judgments[col] = judgments[col].apply(lambda x: str(x).lower())
        return judgments

def split_unit_annotation_score(unit_scores_df):

    col = unit_scores_df['unit_annotation_score']
    scores_true = []
    scores_false = []

    for ind, sc in col.items():
        scores_true.append(sc['true'])
        scores_false.append(sc['false'])
    unit_scores_df['unit_annotation_score_true'] = scores_true
    unit_scores_df['unit_annotation_score_false'] = scores_false


def check_data(data_dict_list):
    clean_data_dict_list = []
    for d in data_dict_list:
        if 'concept' not in d.keys():
            print('concept not found:')
            print(d)

def main():
    runs = ['3', '4', '5_pilot']
    batch = '*'
    n_q = '*'
    group = 'experiment*'

    name = f'run{"_".join(runs)}-group_{group}-batch{batch}'.replace('*', '-all-')
    print(name)
    n_lists = '*'
    data_dict_list = []
    for run in runs:
        data_dict_list.extend(load_experiment_data(run, group, n_q, n_lists, batch, remove_not_val = True))
    print('checking if concepts are there:')
    check_data(data_dict_list)
    add_time_info(data_dict_list)
    print('creating input')
    input_df = create_input_df(data_dict_list)
    input_dir = '../analyses/crowdtruth/input/'
    input_path = f'{input_dir}{name}.csv'
    os.makedirs(input_dir, exist_ok=True)
    input_df.to_csv(input_path, index = False)

    res_dir = '../analyses/crowdtruth/results/'
    res_path = f'{res_dir}{name}'
    os.makedirs(res_dir, exist_ok=True)

    print('running crowdtruth')
    input_file = input_path
    data, config = crowdtruth.load(
        file = input_file,
        config = TestConfig()
    )
    results = crowdtruth.run(data, config)
    print('crowdtruth done')
    unit_scores = results['units']
    split_unit_annotation_score(unit_scores)
    unit_scores.to_csv(f'{res_path}-units.csv')

    worker_scores = results['workers']
    worker_scores.to_csv(f'{res_path}-workers.csv')

    annotation_scores = results["annotations"]
    annotation_scores.to_csv(f'{res_path}-annotations.csv')
    print(f'results stored: {res_path}')




if __name__ == '__main__':
    main()
