import pandas as pd
import crowdtruth
from crowdtruth.configuration import DefaultConfig
import os

from utils_data import load_processed_data

import pandas as pd
import os
from collections import defaultdict

from datetime import datetime, time, timedelta
from utils_analysis import sort_by_key


def adapt_time_info(d):
    submitted = d['timestamp_datetime']
    f = '%Y-%m-%d %H:%M:%S'
    submitted_dt = datetime.strptime(submitted, f)
    seconds = d['duration_in_seconds']
    if seconds != '':
        s = int(float(seconds))
    else:
        s = 0
    seconds_dt = timedelta(seconds=s)
    started = submitted_dt - seconds_dt

    d['_started_at'] = started
    d['_created_at'] = submitted


def create_input_df(data):
    """
    Create final dataframe in format expected by crowdtruth (figure8)

    :param list all_question_dicts: List of all annotated units with time info
    :return: dataframe in expected format
    """
    [adapt_time_info(d) for d in data]
    final_df = pd.DataFrame.from_records(data)
    # make name changes so the format will be recognized
    final_df.rename(columns={'quid': '_unit_id'}, inplace=True)
    final_df.rename(columns={'id': '_id'}, inplace=True)
    final_df.rename(columns={'workerid': '_worker_id'}, inplace=True)
    headers_to_drop = ['assignmentid',
                       'completionurl', 'exampletrue', 'examplefalse', 'filename', 'hitid',
                       'listnumber', 'origin', 'partid', 'questionid', 'run', 'sublist', 'timestamp']
    headers_to_drop_in_df = []
    for h in headers_to_drop:
        if h in final_df.columns:
            headers_to_drop_in_df.append(h)
    final_df.drop(columns=headers_to_drop_in_df, inplace=True)

    col_order = ['_unit_id', '_id', '_worker_id', '_started_at',
                 '_created_at', 'relation', 'concept',
                 'property', 'answer', ]
    final_df = final_df[col_order]
    return final_df


class TestConfig(DefaultConfig):
    inputColums = ['relation', 'property', 'concept']
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
    runs = ['3', '4']
    #sources = ['raw_processed', 'clean_contradictions_batch_0.5']
    sources = ['data_processed', 'clean_contradictions_total_1', 'clean_contradictions_pair_0.5', 'clean_contradictions_batch_0.5']
    group = 'experiment*'
    n_q = '*'
    n_lists = '*'
    batch = '*'

    #name = f'run{"_".join(runs)}-group_{group}-batch{batch}'.replace('*', '-all-')
    n_lists = '*'
    for source in sources:
        data_dict_list = []
        name = f'run{"_".join(runs)}-group_{group}-batch{batch}'.replace('*', '-all-')
        name = f'{name}-{source}'
        print(name)
        for run in runs:
            print(run)
            data = load_processed_data(run, group, n_q, n_lists,
                                       batch, source)
            data_dict_list.extend(data)
            # data_dict_list.extend(load_experiment_data(run, group, n_q, n_lists, batch, remove_not_val = True))
        print(len(data_dict_list))
        print('checking if concepts are there:')
        check_data(data_dict_list)

        print('creating input')
        input_df = create_input_df(data_dict_list)
        print(input_df.columns)
        input_dir = '../analyses/crowdtruth/input/'
        input_path = f'{input_dir}{name}.csv'
        os.makedirs(input_dir, exist_ok=True)
        input_df.to_csv(input_path, index=False)

        res_dir = '../analyses/crowdtruth/results/'
        res_path = f'{res_dir}{name}'
        os.makedirs(res_dir, exist_ok=True)

        print('running crowdtruth')
        input_file = input_path
        data, config = crowdtruth.load(
            file=input_file,
            config=TestConfig()
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
