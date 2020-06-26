import glob
import csv
import uuid
import os
from utils_analysis import sort_by_key
import pandas as pd

import json
from io import StringIO

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





def match_ids(dict_list_out_batch, dict_list_sum_batch, remove_not_val = True, v=False):

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

def add_time_info(dict_list_out_batch, dict_list_sum_batch):
    if dict_list_sum_batch != []:
        worker_time_dict = {d['participant_id']: d['time_taken'] for d in dict_list_sum_batch}
    else:
        worker_time_dict = dict()
    for d in dict_list_out_batch:
        worker = d['workerid']
        if worker in worker_time_dict:
            time = worker_time_dict[worker]
        else:
            time = 0.0
        d['time_taken_batch'] = time


def process_triple_and_answer(dict_list_out):

    for d in dict_list_out:
        answer = d['answer'].strip()

        #answer_dict = parse_answer(answer)
        answer_dict = load_answer(answer)
        #print(answer_dict)
        d.pop('answer')
        d.update(answer_dict)
        rel, prop, concept = d.pop('triple').split('-')
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


def add_unique_ids(exp_path):
    path_output = f'../data/prolific_output/{exp_path}'
    path_output_uuid = f'../data/prolific_output_uuid/{exp_path}'
    if not os.path.isfile(path_output_uuid):
        print('creating file with uuid', exp_path)
        dir_exp = exp_path.split('/')[0]
        dict_list = load_batch_data_raw(path_output)
        for d in dict_list:
            d['uuid'] = uuid.uuid4()
        os.makedirs(f'../data/prolific_output_uuid/{dir_exp}/', exist_ok=True)
        write_batch_data_uuid(dict_list, path_output_uuid)
        print(f'added unique ids - file can be found at: {path_output_uuid}')


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
            if relation != '_check' and not relation.startswith('test_'):
                continue
            else:
                clean_data.extend(dl)
        else:
            clean_data.extend(dl)
    if v == True:
        print(f'number of questions: {len(data_dict_list)}')
        print(f'number of questions without singletons: {len(clean_data)}')
    return clean_data

def load_experiment_data(run, group, n_q, batch, remove_not_val = True):

    all_dict_list_out = []

    dir_output = '../data/prolific_output/'
    all_files = f'{dir_output}run{run}-group_{group}/qu{n_q}-s_qu{n_q}-batch{batch}.csv'
    annotations_discarded = 0.0
    for f in glob.glob(all_files):
        # check if files already have unique ids
        exp_path = f[len(dir_output):]
        add_unique_ids(exp_path)
        dict_list_sum_batch = load_experiment_summaries_batch(exp_path, remove_not_val = remove_not_val)
        if dict_list_sum_batch != []:
            dict_list_out_batch = load_experiment_data_batch(exp_path, remove_not_val = remove_not_val)
            match_ids(dict_list_out_batch, dict_list_sum_batch, remove_not_val = True, v=False)
            add_time_info(dict_list_out_batch, dict_list_sum_batch)
            process_triple_and_answer(dict_list_out_batch)
            dict_list_out_clean = remove_singletons(dict_list_out_batch)
            annotations_discarded += len(dict_list_out_batch) - len(dict_list_out_clean)
            all_dict_list_out.extend(dict_list_out_clean)
    print(f'Discarded {annotations_discarded} annotations.')
    return all_dict_list_out

def load_gold_data_batch(exp_path):
    dir_output = '../gold_labels/gold_files'
    with open(f'{dir_output}/{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile, delimiter = '\t'))
    return dict_list_out

def load_gold_data(run, group):

    name = name = f'run{run}-{group}.csv'.replace('*', '-all-')
    gold_path = f'../gold_labels/gold_files/{name}'
    df = pd.read_csv(gold_path)
    dict_list = df.to_dict('records')

    return dict_list


def load_expert_data_batch(exp_path):
    dir_output = '../data/prolific_output_uuid/'
    with open(f'{dir_output}{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile, delimiter = ','))
    return dict_list_out

def load_expert_data(run, group, n_q, batch):
    all_dict_list_out = []
    dir_output = '../data/prolific_output/'
    name = f'run{run}-group_{group}/qu{n_q}-s_qu{n_q}-batch{batch}'
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

def main():
    run = 3
    batch = 16
    n_q = 70
    group = 'experiment1'

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)


if __name__ == '__main__':
    main()
