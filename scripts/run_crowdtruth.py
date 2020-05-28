import pandas as pd
import crowdtruth
from crowdtruth.configuration import DefaultConfig
import os


from load_data import load_experiment_data


import pandas as pd
import os
from collections import defaultdict
import datetime
import pytz
import json

def create_time_series(question_dicts, day):
    """
    Create a time series from all the questions answered by a single worker.

    :param list question_dicts: List of dicts with question info
    :param str day: date information
    :return: tuples (containing time and position in original list)
    """
    # create_time_serious of question_dicts
    time_tuples = []
    #print('number of questions:', len(question_dicts))
    for n, qu in enumerate(question_dicts):
        time = qu['timestamp']
        d, time = time.split(' ')
        time = datetime.time.fromisoformat(time)
        dt = datetime.datetime.combine(day, time)
        time_tuple = (dt, n)
        time_tuples.append(time_tuple)
    return time_tuples


def add_start_end_times(time_tuples, question_dicts, start_dt_a):
    """
    Add start and end times to time series.

    :param list time tuples: List of tuples with submission time
    and original position in question_dicts (list)
    :param list question_dicts: list of all questions answered by one participant
    :param datetime.datetime: starting time (Amsterdam timezone)
    :return: list of all question dicts including time stat and end time
    """
    all_questions = []
    times = sorted(time_tuples)
    for n, time_i in enumerate(times):
        dt, i = time_i
        qu = question_dicts[i]
        if n == 0:
            time_start = start_dt_a
        else:
            time_start = time_tuples[n-1][0]
        time_finish = dt
        qu['_started_at'] = str(time_start)
        qu['_created_at'] = str(time_finish)
        all_questions.append(qu)
    return all_questions

def add_time_info(question_dicts, start_dicts=None):
    """
    Add start and end times to time series.

    :param list time tuples: List of tuples with submission time
    and original position in question_dicts (list)
    :param list question_dicts: list of all questions answered by one participant
    :return: list of all question dicts including time stat and end time
    """
    # level of a single participant within a batch
    # We have start and end time of the participant working on a batch
    if start_dicts != None:
        start = start_dicts[0]['started_datetime']
    else:
        start = '2010-01-01 00:00:00.000000'
    if type(start) == str:
        start_dt = datetime.datetime.fromisoformat(start).replace(tzinfo=pytz.timezone('Europe/London'))
        day = start_dt.date()
        start_dt_a = start_dt.astimezone(pytz.timezone('Europe/Amsterdam')).replace(tzinfo=None).replace(microsecond=0)
        time_tuples = create_time_series(question_dicts, day)
        all_questions = add_start_end_times(time_tuples, question_dicts, start_dt_a)
    else:
        # not encountered so far
        print('start not string')
        all_questions = []
    return all_questions



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
    final_df.drop(columns = ['assignmentid',
    'completionurl', 'exampletrue', 'examplefalse', 'filename', 'hitid',
    'listnumber', 'origin', 'partid', 'questionid', 'run', 'sublist', 'timestamp'],
    inplace = True)
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



if __name__ == '__main__':

    run = '*'
    batch = '*'
    n_q = '*'
    group = 'experiment*'

    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    print('checking if concepts are there:')
    check_data(data_dict_list)
    data_dicts_time = add_time_info(data_dict_list)
    print('creating input')
    input_df = create_input_df(data_dicts_time)
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

    unit_scores = results['units']
    split_unit_annotation_score(unit_scores)
    unit_scores.to_csv(f'{res_path}-units.csv')

    worker_scores = results['workers']
    worker_scores.to_csv(f'{res_path}-workers.csv')

    annotation_scores = results["annotations"]
    annotation_scores.to_csv(f'{res_path}-annotations.csv')
