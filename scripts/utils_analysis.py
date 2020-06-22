from collections import defaultdict
from collections import Counter
import pandas as pd


def sort_by_key(data_dict_list, keys):

    sorted_dict = defaultdict(list)
    for d in data_dict_list:
        if len(keys) == 1:
            key = keys[0]
            sortkey = d[key]
        else:
            sortkeys = []
            for key in keys:
                sortkey = d[key]
                sortkeys.append(sortkey)
            sortkey = '-'.join(sortkeys)
        sorted_dict[sortkey].append(d)
    return sorted_dict


def get_relation_cnt(pair_dicts):
    relation_cnt = Counter()
    for d in pair_dicts:
        answer = str(d['answer']).lower()
        if answer == 'true':
            val = 1
        else:
            val = 0
        relation_cnt[d['relation']] += val

    return relation_cnt

def get_relation_pairs(pair_dicts, threshold = 0):
    #print(pair_dicts)
    relation_cnt = get_relation_cnt(pair_dicts)
    #print('relation count', relation_cnt)
    relations_true = [rel for rel, cnt in relation_cnt.items() if cnt > threshold]

    relation_pairs = []
    for rel1 in relations_true:
        for rel2 in relations_true:
            pair = set([rel1, rel2])
            if len(pair) > 1 and pair not in relation_pairs:
                relation_pairs.append(pair)
    return relation_pairs


def collect_contradictions(pair_dicts, contradictions, threshold = 0):
    relation_pairs = get_relation_pairs(pair_dicts, threshold = threshold)
    #print('relation pairs', relation_pairs)
    contradiction_pairs = [tuple(sorted(p)) for p in relation_pairs if p in contradictions]
    return contradiction_pairs


def load_contradiction_pairs():
    contradictions = []
    with open('../scheme/contradictions.csv') as infile:
        for line in infile:
            contradictions.append(set(line.strip('\n').split(',')))
    return contradictions

def get_annotation_ids(dict_list):
    ids = []
    for d in dict_list:
        uuid = d['uuid']
        ids.append(uuid)
    return ids


def load_analysis(analysis_type, run, exp_name, batch, as_dict = False):
    #../analyses/pairs/run-all--group_experiment1-batch-all-.csv
    # run3-group_experiment1-batch53.csv
    path = f'../analyses/{analysis_type}/run{run}-group_{exp_name}-batch{batch}.csv'
    path = path.replace('*', '-all-')
    analysis = pd.read_csv(path)
    if as_dict == True:
        analysis = analysis.to_dict('records')
    return analysis


def load_ct(run, exp_name, batch, analysis_type, as_dict=False):
    dir = '../analyses/crowdtruth/results/'
    path = f'{dir}run{run}-group_{exp_name}-batch{batch}-{analysis_type}.csv'
    path = path.replace('*', '-all-')
    analysis = pd.read_csv(path)
    if as_dict == True:
        analysis = analysis.to_dict('records')
    return analysis
