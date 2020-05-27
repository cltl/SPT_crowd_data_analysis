# check collapsed agreement


from load_data import load_experiment_data
from utils_analysis import sort_by_key
from nltk import agreement
from sklearn.metrics import cohen_kappa_score
import numpy as np
import csv
from collections import defaultdict
import numpy as np


def load_rel_level_mapping(mapping = 'levels'):
    # load mapping
    rel_level_dict = dict()
    with open(f'../scheme/relation_overview_run4.csv') as infile:
            mapping_dicts = list(csv.DictReader(infile, delimiter = '\t'))

    if mapping == 'levels':
        for d in mapping_dicts:
            rel = d['relation']
            l = d['level']
            rel_level_dict[rel] = l

    elif mapping == 'pos_neg':
        for d in mapping_dicts:
            rel = d['relation']
            l = d['level']
            if l in ['all', 'some']:
                rel_level_dict[rel] = 'pos'
            else:
                rel_level_dict[rel] = 'neg'
    elif mapping == 'similar_relations':
        for d in mapping_dicts:
            rel = d['relation']
            l = d['level']
            if rel in ['variability_limited', 'variability_open']:
                rel_level_dict[rel] = 'variability'
            elif rel in ['unusual', 'rare']:
                rel_level_dict[rel] = 'unusual_rare'
            else:
                rel_level_dict[rel] = rel

    return rel_level_dict

def get_collapsed_relations(dict_list, mapping = 'levels'):

    collapsed_dicts = []
    level_rel_dict = load_rel_level_mapping(mapping = mapping)
    dict_list_by_worker = sort_by_key(dict_list, ['workerid'])
    for w, dicts in dict_list_by_worker.items():
        dicts_by_level = defaultdict(list)
        for d in dicts:
            rel = d['relation']
            if rel in level_rel_dict:
                level = level_rel_dict[rel]
                dicts_by_level[level].append(d)
        for level, dicts in dicts_by_level.items():
            new_d = dict()
            dicts_by_pair = sort_by_key(dicts, ['property', 'concept'])
            for pair, p_dicts in dicts_by_pair.items():
                new_d['quid'] = f'{pair}-{level}'
                new_d['workerid'] = w
                new_d['completionurl'] = p_dicts[0]['completionurl']
                answers = [d['answer'] for d in p_dicts]
                if 'true' in answers:
                    new_d['answer'] = 'true'
                else:
                    new_d['answer'] = 'false'
                collapsed_dicts.append(new_d)
    return collapsed_dicts




def create_matrix(dict_list):
    quid_dict = defaultdict(list)
    for d in dict_list:
        quid = d['quid']
        quid_dict[quid].append(d)

    all_rows = []
    for quid, ds in quid_dict.items():
        for n, d in enumerate(ds):
            worker = d['workerid']
            answer = d['answer']
            row = [worker, quid, answer]
            all_rows.append(row)
    return all_rows



def proportional_agreement_pairs(matrix):
    """
    data: list of triples representing instances: (worker, unit, label)
    """

    unit_dict = defaultdict(dict)
    agreements = 0.0

    all_labels = set()
    for w, u, l in matrix:
        all_labels.add(l)
        unit_dict[u][w] = l

    for u, worker_judgment_dict in unit_dict.items():
        #n_annotators = len(judgements)
        ag_cnt = 0.0
        workers = worker_judgment_dict.keys()
        pairs = coder_pairs_unit(workers)
        for i, j in pairs:
            if i in worker_judgment_dict and j in worker_judgment_dict:
                li = worker_judgment_dict[i]
                lj = worker_judgment_dict[j]
                if li == lj:
                    ag_cnt += 1
        if ag_cnt != 0:
            agreement_unit = ag_cnt /len(pairs)
        else:
            agreement_unit = 0
        agreements += agreement_unit
    overall = agreements/len(unit_dict)
    return overall




def coder_pairs_unit(workers):

    pairs = set()
    for i in workers:
        for j in workers:
            if i != j:
                pair = (i, j)
                pair_rev = (j, i)
                if pair_rev not in pairs:
                    pairs.add(pair)
    return pairs

def coder_pairs_matrix(matrix):

    workers = set([m[0] for m in matrix])

    pairs = set()
    for i in workers:
        for j in workers:
            if i != j:
                pair = (i, j)
                pair_rev = (j, i)
                if pair_rev not in pairs:
                    pairs.add(pair)
    return pairs


def get_average_kappa(matrix):
    pair_kappa_dict = get_kappa_pairs(matrix)

    if len(pair_kappa_dict) > 0:
        sum_kappa = sum(pair_kappa_dict.values())
        av_kappa = sum_kappa/len(pair_kappa_dict)
    else:
        av_kappa = 0
    return av_kappa


def get_kappa_pairs(matrix):
    pairs = coder_pairs_matrix(matrix)
    unit_dict = defaultdict(dict)
    pair_unit_dict = defaultdict(list)
    pair_kappa_dict = dict()
    for w, u, l in matrix:
        unit_dict[u][w] = l
    all_pair_answers = []
    sum_kappas =0.0
    sum_valid_pairs = 0.0
    for wi, wj in pairs:
        pair_label_dict = defaultdict(list)
        for u, worker_l_dict in unit_dict.items():
            if wi in worker_l_dict and wj in worker_l_dict:
                pair_label_dict[wi].append(worker_l_dict[wi])
                pair_label_dict[wj].append(worker_l_dict[wj])
        labels_i = pair_label_dict[wi]
        labels_j = pair_label_dict[wj]
        if len(labels_i) > 0:
            kappa = cohen_kappa_score(labels_i, labels_j)
            pair_kappa_dict[(wi, wj)] = kappa

    return pair_kappa_dict


def get_alpha(dict_list_out, collapse_relations = False):
    if collapse_relations != False:
        dict_list_out = get_collapsed_relations(dict_list_out, collapse_relations)
    matrix = create_matrix(dict_list_out)
    ratingtask = agreement.AnnotationTask(data=matrix)
    alpha = ratingtask.alpha()
    return alpha

def get_agreement(dict_list_out, collapse_relations = False, v=True, disable_kappa=False):
    agreement_dict = dict()
    if collapse_relations != False:
        dict_list_out = get_collapsed_relations(dict_list_out, collapse_relations)
    matrix = create_matrix(dict_list_out)
    ratingtask = agreement.AnnotationTask(data=matrix)
    alpha = ratingtask.alpha()
    prop = proportional_agreement_pairs(matrix)
    #average_kappa = get_average_kappa(matrix)
    # Calculate kappa by file (not over entire set)
    total_kappa = 0.0
    data_by_file = sort_by_key(dict_list_out, ['completionurl'])
    for f, d_list in data_by_file.items():
        matrix = create_matrix(d_list)
        if disable_kappa == False:
            kappa = get_average_kappa(matrix)
            if np.isnan(kappa):
                kappa = 0.0
            total_kappa += kappa
        else:
            kappa = '-'

    if total_kappa != 0.0 and len(data_by_file) != 0 and kappa != '-':
        average_kappa = total_kappa/len(data_by_file)
    else:
        average_kappa = '-'
    if v == True:
        print(f"Krippendorff's alpha: {alpha}")
        print(f"Average Cohen's Kappa (pairwise): {average_kappa}")
        print(f"Proportional agreement (pairwise): {prop}")
        print()
    agreement_dict['Krippendorff'] = alpha
    agreement_dict['Proportional'] = prop
    agreement_dict['Av_Cohens_kappa'] = average_kappa
    return agreement_dict


def get_full_report(dict_list_out, v=False):

    full_ag_dict = dict()
    versions = ['pos_neg', 'levels', 'similar_relations']
    if v == True:
        print(f'--- Full IAA report --- ')
        print('Full set:')
    full_ag_dict['full'] = get_agreement(dict_list_out, v=v)
    for version in versions:
        if v == True:
            print(f'collapsing {version}')
        full_ag_dict[version] = get_agreement(dict_list_out,\
                                            collapse_relations = version, v=v)

    return full_ag_dict



def main():
    run = "4"
    group = 'experiment2'
    batch = '129'
    n_q = '*'

    dict_list_out = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    get_full_report(dict_list_out, v=True)


if __name__ == '__main__':
    main()
