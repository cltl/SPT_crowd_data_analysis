from load_data import load_experiment_data
from nltk import agreement

import csv
from collections import defaultdict

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

def get_agreement(dict_list_out):
    matrix = create_matrix(dict_list_out)
    ratingtask = agreement.AnnotationTask(data=matrix)
    alpha = ratingtask.alpha()
    prop = proportional_agreement_pairs(matrix)
    print(f"Krippendorff's alpha: {alpha}")
    print(f"Proportional agreement (pairwise): {prop}")
    print()

def main():
    run = 1
    group = 'experiment1'
    batch = '*'
    n_q = '*'
    print(f'--- analyzing run {run} ---')
    dict_list_out = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    get_agreement(dict_list_out)

if __name__ == '__main__':
    main()
