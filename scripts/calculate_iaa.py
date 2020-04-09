# check collapsed agreement


from load_data import load_experiment_data
from utils_analysis import sort_by_key
from nltk import agreement

import csv
from collections import defaultdict



def load_rel_level_mapping(mapping = 'levels'):
    # load mapping
    rel_level_dict = dict()
    with open('../scheme/relation_overview_run3.csv') as infile:
            mapping_dicts = list(csv.DictReader(infile))

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
            worker = n
            answer = d['answer']
            row = [worker, quid, answer]
            all_rows.append(row)
    return all_rows

def coder_pairs(n_annotators):

    annotators = list(range(n_annotators))
    pairs = set()
    for i in annotators:
        for j in annotators:
            if i != j:
                pair = (i, j)
                pair_rev = (j, i)
                if pair_rev not in pairs:
                    pairs.add(pair)
    return pairs

def proportional_agreement_pairs(matrix):
    """
    data: list of triples representing instances: (worker, unit, label)
    """

    unit_dict = defaultdict(list)
    agreements = 0.0

    all_labels = set()
    for w, u, l in matrix:
        all_labels.add(l)
        unit_dict[u].append(l)

    for u, judgements in unit_dict.items():
        n_annotators = len(judgements)
        pairs = coder_pairs(n_annotators)
        ag_cnt = 0.0
        for i, j in pairs:
            li = judgements[i]
            lj = judgements[j]
            if li == lj:
                ag_cnt += 1
        if ag_cnt != 0:
            agreement_unit = ag_cnt /len(pairs)
        else:
            agreement_unit = 0
        agreements += agreement_unit
    overall = agreements/len(unit_dict)
    return overall

def get_agreement(dict_list_out, collapse_relations = False, v=True):
    agreement_dict = dict()
    if collapse_relations != False:
        print(collapse_relations)
        dict_list_out = get_collapsed_relations(dict_list_out, collapse_relations)
    matrix = create_matrix(dict_list_out)
    ratingtask = agreement.AnnotationTask(data=matrix)
    alpha = ratingtask.alpha()
    prop = proportional_agreement_pairs(matrix)
    if v == True:
        print(f"Krippendorff's alpha: {alpha}")
        print(f"Proportional agreement (pairwise): {prop}")
        print()
    agreement_dict['Krippendorff'] = alpha
    agreement_dict['Proportional'] = prop
    return agreement_dict


def main():
    run = 3
    group = 'experiment1'
    batch = '*'
    n_q = '*'
    print(f'--- analyzing run {run} ---')
    dict_list_out = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    get_agreement(dict_list_out)

    print(f'--- analyzing run {run} --- ')

    collapse_relations = 'pos_neg'
    print(f'collapsing {collapse_relations}')
    get_agreement(dict_list_out, collapse_relations = collapse_relations)

    collapse_relations = 'levels'
    print(f'collapsing {collapse_relations}')
    get_agreement(dict_list_out, collapse_relations = collapse_relations)

    collapse_relations = 'similar_relations'
    print(f'collapsing {collapse_relations}')
    get_agreement(dict_list_out, collapse_relations = collapse_relations)

if __name__ == '__main__':
    main()
