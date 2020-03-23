import csv
import glob
from collections import defaultdict


def parse_answer(answer):
    # "{\\"7\\":{\\"answer\\":false}}"
    clean_answer = answer.split(':')[-1].rstrip('}}"')
    return clean_answer

def sort_by_workers(dict_list):
    worker_dict = defaultdict(list)
    for d in dict_list:
        if 'workerid' in d:
            worker_id = d['workerid'].strip()
        elif 'participant_id' in d:
            worker_id = d['participant_id'].strip()
        worker_dict[worker_id].append(d)
    return worker_dict

def remove_not_valied(dict_list_out, dict_list_sum):
    worker_dict_out = sort_by_workers(dict_list_out)
    status_include = ['AWAITING REVIEW', 'APPROVED']
    for d in dict_list_sum:
        worker = d['participant_id']
        status = d['status'].strip()
        if status not in status_include and worker in worker_dict_out:
            worker_dict_out.pop(worker)
    # [j for sub in ini_list for j in sub]
    dict_list_out_clean = [d for dict_list in worker_dict_out.values() for d in dict_list]
    return dict_list_out_clean


def load_experiment_data_batch(exp_path, remove_not_val = True):

    dir_output = '../data/prolific_output/'
    with open(f'{dir_output}/{exp_path}') as infile:
        dict_list_out = list(csv.DictReader(infile))
    if remove_not_val == True:
        dir_summary = '../data/prolific_summaries/'
        with open(f'{dir_summary}/{exp_path}') as infile:
            dict_list_sum = list(csv.DictReader(infile))
        dict_list_out = remove_not_valied(dict_list_out, dict_list_sum)
    return dict_list_out

def replace_id(dict_list_out, mapping_dict, v=False):

    for d in dict_list_out:
        workerid = d['workerid']
        if workerid == mapping_dict['out']:
            d['workerid'] = mapping_dict['summary']
            if v == True:
                print(f'Replaced worker id {workerid} with {mapping_dict["summary"]}')


def load_experiment_data(run, group, n_q, batch, remove_not_val = True):

    all_dict_list_out = []

    dir_output = '../data/prolific_output/'
    all_files = f'{dir_output}run{run}-group_{group}/qu{n_q}-s_qu{n_q}-batch{batch}.csv'

    for f in glob.glob(all_files):
        exp_path = f[len(dir_output):]
        dict_list_out = load_experiment_data_batch(exp_path, remove_not_val = remove_not_val)
        mapping_dict = get_id_mapping(exp_path, remove_not_val = True)
        if mapping_dict != None:
            replace_id(dict_list_out, mapping_dict)
        all_dict_list_out.extend(dict_list_out)
    return all_dict_list_out


def load_experiment_summaries_batch(exp_path, remove_not_val = True):

    dir_summary = '../data/prolific_summaries'
    with open(f'{dir_summary}/{exp_path}') as infile:
        dict_list_sum = list(csv.DictReader(infile))

    if remove_not_val == True:
        dir_summary = '../data/prolific_summaries/'
        with open(f'{dir_summary}/{exp_path}') as infile:
            dict_list_sum = list(csv.DictReader(infile))
        dict_list_sum = remove_not_valied(dict_list_sum, dict_list_sum)
    return dict_list_sum


def load_experiment_summaries(run, group, n_q, batch, remove_not_val = True):

    all_dict_list_sum = []

    dir_sum = '../data/prolific_summaries'
    all_files = f'{dir_sum}/run{run}-group_{group}/qu{n_q}-s_qu{n_q}-batch{batch}.csv'

    for f in glob.glob(all_files):
        exp_path = f[len(dir_sum):]
        dict_list_sum = load_experiment_summaries_batch(exp_path, remove_not_val = remove_not_val)
        all_dict_list_sum.extend(dict_list_sum)
    return all_dict_list_sum


def get_pair_dict(dict_list):
    pair_dict = defaultdict(dict)
    for d in dict_list:
        triple = d['triple']
        answer = parse_answer(d['answer'])
        relation, prop, concept = triple.split('-')
        rel_dict = pair_dict[(prop, concept)]
        if relation in rel_dict:
            rel_dict[relation].append(answer)
        else:
            rel_dict[relation] = [answer]
    return pair_dict

def load_contradiction_pairs():
    contradictions = []
    with open('../scheme/contradictions.csv') as infile:
        for line in infile:
            contradictions.append(set(line.strip('\n').split(',')))
    return contradictions

def get_relation_counts(relation_vecs, normalize = True):
    relation_dict = dict()
    for rel, rel_vec in relation_vecs.items():
        true_cnt = rel_vec.count('true')
        total_cnt = len(rel_vec)
        if normalize == True:
            val = true_cnt/total_cnt
        else:
            val = true_cnt
        relation_dict[rel] = val
    return relation_dict

def consistency_check(contradiction_pairs, relation_counts, thresh = 0.5):
    rel_above_thresh = [rel for rel, cnt in relation_counts.items() if cnt > thresh]
    contradictions = []
    for r1 in rel_above_thresh:
        for r2 in rel_above_thresh:
            if r1 != r2:
                pair = set([r1, r2])
                if pair in contradiction_pairs and pair not in contradictions:
                    contradictions.append(pair)
    return contradictions

def get_worker_dict(dict_list_out):
    worker_dict = defaultdict(list)
    for d in dict_list_out:
        worker = d['workerid']
        worker_dict[worker].append(d)
    return worker_dict

def get_worker_pair_dict(dict_list_out):
    worker_dict = get_worker_dict(dict_list_out)
    worker_pair_dict = dict()
    for worker, worker_dict_list in worker_dict.items():
        pair_dict = get_pair_dict(worker_dict_list)
        worker_pair_dict[worker] = pair_dict
    return worker_pair_dict


def get_id_mapping(exp_path, remove_not_val = True):
    dict_list_sum_batch = load_experiment_summaries_batch(exp_path,\
                                                          remove_not_val = remove_not_val)
    dict_list_out_batch = load_experiment_data_batch(exp_path,\
                                                     remove_not_val = remove_not_val)

    worker_ids_sum = set([d['participant_id'] for d in dict_list_sum_batch])
    worker_ids_out = set([d['workerid'] for d in dict_list_out_batch])

    w_in_summary_only = worker_ids_sum.difference(worker_ids_out)
    w_in_out_only = worker_ids_out.difference(worker_ids_sum)
    matching_ids = worker_ids_sum.intersection(worker_ids_out)

    if len(w_in_summary_only) == 1 and len(w_in_out_only) == 1:
        mapping_dict = dict()
        mapping_dict['out'] = list(w_in_out_only)[0]
        mapping_dict['summary'] = list(w_in_summary_only)[0]
    else:
        mapping_dict = None

    #print(f'{len(matching_ids)} out of {len(worker_ids_sum)} workerids match.')
    return mapping_dict
