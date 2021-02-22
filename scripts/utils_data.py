import argparse

import glob
import csv
import uuid
import os
from utils_analysis import sort_by_key
import pandas as pd
import json
from datetime import datetime
from datetime import timedelta


def load_config():
    path = '../config.json'
    with open(path) as infile:
        config_dict = json.load(infile)
    return config_dict

def parse_answer(answer):
    # "{\"14\":{\"answer\":false
    # false
    #\"crowd\":true}}"
    #true
    answer_dict = dict()
    answers = answer.split(',\\')
    for answer in answers:
        answer = answer.split('\\":')
        if len(answer) == 3:
            cat, answer = answer[1:]
        elif len(answer) == 2:
            cat, answer = answer
        else:
            print('invalid answer:', answer)
            cat = 'invalid'
            answer = 'invalid'
        cat = cat.strip('{\"').strip('\\"')
        answer  = answer.strip('\\"').strip('\"}}"').strip('\\')
        answer_dict[cat] = answer
    return answer_dict


def load_answer(answer):
    #print('----input string----')

    #print('----stripping string----')
    #answer = answer.replace('"', "'")
    answer = answer[1:-1].replace('\\', '')
    #print(answer)
    #print()
    #answer = answer.replace('"Time flies like an arrow"', 'Time flies like an arrow')
    # clean quotes from free text
    #print(answer)

    answer = answer.split(':{')[1].split('}}')[0]
    pairs = answer.split(',"')
    pair_tuples = [tuple(p.strip('"').split('":')) for p in pairs if '":' in p]
    pair_tuples = [(k.strip(), v.strip()) for k, v in pair_tuples]
    #print(pair_tuples)

    answer_dict = dict(pair_tuples)
    return answer_dict





def match_ids(dict_list_out_batch, dict_list_sum_batch, remove_not_val = True, v=True):

    if len(dict_list_sum_batch) != 0:
        worker_ids_sum = set([d['participant_id'] for d in dict_list_sum_batch])
        worker_ids_out = set([d['workerid'] for d in dict_list_out_batch])

        w_in_summary_only = worker_ids_sum.difference(worker_ids_out)
        w_in_out_only = worker_ids_out.difference(worker_ids_sum)
        matching_ids = worker_ids_sum.intersection(worker_ids_out)

        if v == True:
            print(f'{len(matching_ids)} out of {len(worker_ids_out)} match.')

        if len(w_in_summary_only) == 1 and len(w_in_out_only) == 1:
            mapping_dict = dict()
            mapping_dict['out'] = list(w_in_out_only)[0]
            mapping_dict['summary'] = list(w_in_summary_only)[0]
            replace_id(dict_list_out_batch, mapping_dict, v=False)
        elif len(w_in_out_only) == 1 and len(w_in_summary_only) == 0:
            if v == True:
                print('invalid submission in output')
            mapping_dict = dict()
            mapping_dict['out'] = list(w_in_out_only)[0]
            mapping_dict['summary'] = 'NS'
            replace_id(dict_list_out_batch, mapping_dict, v=False)
        else:
            if v == True:
                print(f'mapping not possible or necessary because:')
                print(f'n ids in summary only: {len(w_in_summary_only)}')
                print(f'n ids in output only: {len(w_in_out_only)}')
    else:
        print('no summary data')



def replace_id(dict_list_out, mapping_dict, v=False):

    if mapping_dict['summary'] == 'NS':
        to_remove = []
        for d in dict_list_out:
            workerid = d['workerid']
            if workerid == mapping_dict['out']:
                to_remove.append(d)
        for d in to_remove:
            dict_list_out.remove(d)

    else:
        for d in dict_list_out:
            workerid = d['workerid']
            if workerid == mapping_dict['out']:
                d['workerid'] = mapping_dict['summary']
                if v == True:
                    print(f'Replaced worker id {workerid} with {mapping_dict["summary"]}')

def remove_not_valid(dict_list_out, dict_list_sum):
    #worker_dict_out = sort_by_workers(dict_list_out)
    status_include = ['AWAITING REVIEW', 'APPROVED']
    dict_list_out_clean = []

    workers_revoked = [d['participant_id'] for d in dict_list_sum\
                       if d['status'].strip() not in status_include]

    for d in dict_list_out:
        if 'workerid' in d.keys():
            worker = d['workerid']
        elif 'participant_id' in d.keys():
            worker = d['participant_id']
        if worker not in workers_revoked:
            dict_list_out_clean.append(d)
    # [j for sub in ini_list for j in sub]
    return dict_list_out_clean



def load_experiment_summaries_batch(exp_path, remove_not_val = True):

    dir_summary = '../data/prolific_summaries'
    if os.path.isfile(f'{dir_summary}/{exp_path}'):
        with open(f'{dir_summary}/{exp_path}') as infile:
            dict_list_sum = list(csv.DictReader(infile))
        if remove_not_val == True:
            dir_summary = '../data/prolific_summaries/'
            with open(f'{dir_summary}/{exp_path}') as infile:
                dict_list_sum = list(csv.DictReader(infile))
            dict_list_sum = remove_not_valid(dict_list_sum, dict_list_sum)
    else:
        dict_list_sum = []
    return dict_list_sum




def process_triple_and_answer(dict_list_out):

    for d in dict_list_out:
        answer = d['answer'].strip()

        #answer_dict = parse_answer(answer)
        answer_dict = load_answer(answer)
        #print(answer_dict)
        d.pop('answer')
        d.update(answer_dict)
        rel, prop, concept = d.pop('triple').split('-', 2)
        d['relation'] = rel
        d['property'] = prop
        d['concept'] = concept



def load_batch_data_raw(f):
    with open(f) as infile:
        dict_list = list(csv.DictReader(infile))
    return dict_list

def write_batch_data_uuid(dict_list, f_uuid):
    fieldnames = dict_list[0].keys()
    with open(f_uuid, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames = fieldnames)
        writer.writeheader()
        for d in dict_list:
            writer.writerow(d)


def add_unique_ids(exp_path, overwrite_file = False):
    path_output = f'../data/prolific_output/{exp_path}'
    path_output_uuid = f'../data/prolific_output_uuid/{exp_path}'
    if not os.path.isfile(path_output_uuid) or overwrite_file==True:
        print('creating file with uuid', exp_path)
        dir_exp = exp_path.split('/')[0]
        dict_list = load_batch_data_raw(path_output)
        for d in dict_list:
            d['uuid'] = uuid.uuid4()
        os.makedirs(f'../data/prolific_output_uuid/{dir_exp}/', exist_ok=True)
        write_batch_data_uuid(dict_list, path_output_uuid)
        print(f'added unique ids - file can be found at: {path_output_uuid}')

def load_timestamp_lingoturk(stamp_str):
    # conversion map: https://www.journaldev.com/23365/python-string-to-datetime-strptime
    # translate: 28-Jan-2021 09:38:47
    # day %d
    # month %b
    # year %Y
    # hour %H
    # minutes %M
    # seconds %S
    mapping = '%d-%b-%Y %H:%M:%S'
    stamp = datetime.strptime(stamp_str, mapping)
    return stamp

def add_time_info(dict_list_out_batch, dict_list_sum_batch):
    if dict_list_sum_batch != []:
        worker_time_dict = {d['participant_id']:
                                {'time_taken': d['time_taken'],
                                 'started_datetime': d['started_datetime'],
                                 'completed_datetime': d['completed_date_time']}

                            for d in dict_list_sum_batch}
    else:
        worker_time_dict = dict()
    for d in dict_list_out_batch:
        worker = d['workerid']
        timestamp = d['timestamp']
        timestamp_datetime = load_timestamp_lingoturk(timestamp)
        d['timestamp_datetime'] = timestamp_datetime
        if worker in worker_time_dict:
            time_dict = worker_time_dict[worker]
        else:
            time_dict = {'time_taken': None, 'started_datetime': None, 'completed_datetime': None}
        d.update(time_dict)


def get_duration(worker_data):
    # sort data by time stamp to create sequence
    data_by_timestamp = sort_by_key(worker_data, ['timestamp_datetime'])
    time_series = sorted(data_by_timestamp.keys())
    first_timestamp = time_series[0]

    # get task start and end time from any of the dicts
    start_time_str = worker_data[0]['started_datetime']
    end_time_str = worker_data[0]['completed_datetime']

    # get start and end as datetime objects
    if not start_time_str is None:
        start_uk = datetime.fromisoformat(start_time_str)
        diff_start_1 = first_timestamp - start_uk
        diff_start_1_seconds = diff_start_1.total_seconds()
    else:
        start_uk = None
        diff_start_1_seconds = None
    if not end_time_str is None:
        end_uk = datetime.fromisoformat(end_time_str)
        # adapt time zone from uk to our server
    else:
        end_uk = None

    #print('-----time zone update ----')
    hours_seconds = 60 * 60
    hours1 = hours_seconds
    hours2 = 2 * hours_seconds
    if not diff_start_1_seconds is None:
        if diff_start_1_seconds > hours2:
            #print('2 hour difference')
            to_add = timedelta(hours=2)
        else:
            #print('1 hour difference')
            to_add = timedelta(hours=1)

    if not start_uk is None:
        start = start_uk + to_add
    else:
        start = None

    if not end_uk is None:
        end = end_uk + to_add
    else:
        end = None
    #print(start, end)

    #print('---------------------------')

    for n, timestamp in enumerate(time_series):
        ds = data_by_timestamp[timestamp]
        for nd, d in enumerate(ds):
            if nd + 1 < len(ds):
                # print('submitted at same time?')
                submit_time = ds[nd + 1]['timestamp_datetime']
            elif n == len(time_series) - 1:
                # print('final time step: time stamp to submit time')
                if not end is None:
                    submit_time = end
                else:
                    submit_time = None
            else:

                submit_time = timestamp

            if n == 0:
                if not start is None:
                    # print('assign start to start')
                    start_time = start
                else:
                    start_time = None
            else:
                start_time = time_series[n - 1]
            if None in [start_time, submit_time]:
                duration = None
            else:

                duration = (submit_time - start_time).total_seconds()
                #print(start_time, submit_time, duration)
            d['duration_in_seconds'] = duration

def add_duration_info(dict_list_out_batch):
    """
    Add duration info per item after time
    info has been added from
    prolific summary data.
    """
    # divide data by worker
    data_by_worker = sort_by_key(dict_list_out_batch, ['workerid'])
    for worker, worker_data in data_by_worker.items():
        get_duration(worker_data)

def load_experiment_data_batch(exp_path, remove_not_val = True):

    dir_output = '../data/prolific_output_uuid/'
    with open(f'{dir_output}/{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile))
    if remove_not_val == True:
        dir_summary = '../data/prolific_summaries/'
        if os.path.isfile(f'{dir_summary}/{exp_path}'):
            with open(f'{dir_summary}/{exp_path}') as infile:
                dict_list_sum = list(csv.DictReader(infile))
        else:
            dict_list_sum = []
        dict_list_out = remove_not_valid(dict_list_out, dict_list_sum)
    return dict_list_out


def remove_singletons(data_dict_list, v=False):
    clean_data = []
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    for pair, dl in data_by_pair.items():
        data_by_relation = sort_by_key(dl, ['relation'])
        if len(data_by_relation) == 1:
            relation = list(data_by_relation.keys())[0]
            # print(list(data_by_relation.values()))
            quid = list(data_by_relation.values())[0][0]['quid']
            filter_checks = [(relation =='_check'),
                             relation.startswith('test_'),
                             quid.startswith('test')]
            if not any(filter_checks):
                if v == True:
                    print('Filter true')
                continue
            else:
                clean_data.extend(dl)
        else:
            clean_data.extend(dl)
    if v == True:
        print(f'number of questions: {len(data_dict_list)}')
        print(f'number of questions without singletons: {len(clean_data)}')
    return clean_data

def load_experiment_data(run, group, n_q, n_lists,
                         batch, remove_not_val = True, overwrite_file=False, verbose=False):

    all_dict_list_out = []

    dir_output = '../data/prolific_output/'
    all_files = f'{dir_output}run{run}-group_{group}/qu{n_q}-s_qu{n_lists}-batch{batch}.csv'
    annotations_discarded = 0.0
    for f in glob.glob(all_files):
        if verbose == True:
            print(f)
        # check if files already have unique ids
        exp_path = f[len(dir_output):]
        add_unique_ids(exp_path, overwrite_file=overwrite_file)
        dict_list_sum_batch = load_experiment_summaries_batch(exp_path, remove_not_val = remove_not_val)
        if dict_list_sum_batch != []:
            dict_list_out_batch = load_experiment_data_batch(exp_path, remove_not_val = remove_not_val)
            match_ids(dict_list_out_batch, dict_list_sum_batch, remove_not_val = True, v=False)
            add_time_info(dict_list_out_batch, dict_list_sum_batch)
            add_duration_info(dict_list_out_batch)
            process_triple_and_answer(dict_list_out_batch)
            dict_list_out_clean = remove_singletons(dict_list_out_batch)
            [d.update({'f_name_full': f}) for d in dict_list_out_batch]
            annotations_discarded += len(dict_list_out_batch) - len(dict_list_out_clean)
            all_dict_list_out.extend(dict_list_out_clean)
    print(f'Discarded {annotations_discarded} annotations.')
    return all_dict_list_out



def load_processed_data(run, group, n_q, n_lists,
                         batch, name, verbose = False):

    all_dict_list_out = []

    dir_output = f'../data/annotations_{name}/'
    all_files = f'{dir_output}run{run}-group_{group}/qu{n_q}-s_qu{n_lists}-batch{batch}.csv'
    for f in glob.glob(all_files):
        if verbose == True:
            print(f)
        with open(f) as infile:
            all_dict_list_out.extend(list(csv.DictReader(infile, delimiter = ',')))
    return all_dict_list_out

def load_gold_data_batch(exp_path):
    dir_output = '../gold_labels/gold_files'
    with open(f'{dir_output}/{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile, delimiter = '\t'))
    return dict_list_out

def load_gold_data():
    gold_path = f'../gold_labels/gold.csv'
    df = pd.read_csv(gold_path)
    dict_list = df.to_dict('records')
    return dict_list


def load_expert_data_batch(exp_path):
    dir_output = '../data/prolific_output_uuid/'
    with open(f'{dir_output}{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile, delimiter = ','))
    return dict_list_out

def load_expert_data(run, group, n_q, n_lists, batch):
    all_dict_list_out = []
    dir_output = '../data/prolific_output/'
    name = f'run{run}-group_{group}/qu{n_q}-s_qu{n_lists}-batch{batch}'
    all_files = f'{dir_output}{name}.csv'
    for f in glob.glob(all_files):
        exp_path = f[len(dir_output):]
        print(exp_path)
        add_unique_ids(exp_path)
        dict_list_out_batch = load_expert_data_batch(exp_path)
        dict_list_sum_batch = []
        match_ids(dict_list_out_batch, dict_list_sum_batch, remove_not_val = True, v=False)
        add_time_info(dict_list_out_batch, dict_list_sum_batch)
        process_triple_and_answer(dict_list_out_batch)
        all_dict_list_out.extend(dict_list_out_batch)
    return all_dict_list_out


def annotations_to_file(data_dict_list, run, group, batch, type='raw'):
    data_dir = f'../data/annotations-{type}'
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    keys_to_remove = ['assignmentid', 'hitid', 'time_taken', 'partid']
    df = pd.DataFrame(data_dict_list)
    df.drop(keys_to_remove, axis=1, inplace=True)
    name = f'run{run}-group_{group}-batch{batch}'
    name = name.replace('*', '-all-')
    path = f'{data_dir}/{name}.csv'
    df.to_csv(path)
    print(f'Annotations summarized in one csv: {path}')

def main():

    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    n_lists = '*'
    group = config_dict['group']

    data_dict_list = load_experiment_data(run, group, n_q, n_lists, batch, remove_not_val = True)


if __name__ == '__main__':
    main()
