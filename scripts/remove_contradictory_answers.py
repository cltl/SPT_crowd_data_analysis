import csv
import os
from collections import defaultdict
from collections import Counter

from utils import load_experiment_data
from utils import get_pair_dict
from utils import load_contradiction_pairs
from utils import get_relation_counts
from utils import consistency_check
from utils import get_worker_pair_dict



def get_selected_annotations(dict_list_out, target_worker_pairs):
    dict_list_clean = []
    for d in dict_list_out:
        worker = d['workerid']
        triple_list = d['triple'].split('-')
        worker_pair = (worker, (triple_list[1], triple_list[2]))
        #print(worker_pair)
        if worker_pair in target_worker_pairs:
            dict_list_clean.append(d)
    return dict_list_clean


def print_contradiction_analysis(worker_pairs_to_discard, worker_contradictions):
    workers_contradicting = Counter()
    pairs_contridicting = Counter()
    relation_contradictions = Counter()
    for worker, pair in worker_pairs_to_discard:
        workers_contradicting[worker] += 1
        pairs_contridicting[pair] += 1
    for w, contradictions in worker_contradictions.items():
        for c in contradictions:
            relation_contradictions[tuple(c)] += 1
    print('workers contradicting themselves')
    for w, cnt in workers_contradicting.most_common():
        print(w, cnt)
    print()
    print('pairs with many contradictions')
    for p, cnt in pairs_contridicting.most_common():
        print(p, cnt)
    print()
    print('relation contradictions per worker')
    for w, contradictions in worker_contradictions.items():
        contradiction_cnt = Counter()
        print(f'Contradictions of worker {worker}:')
        for c in contradictions:
            contradiction_cnt[tuple(c)] += 1
        for c, cnt in contradiction_cnt.most_common():
            print(c, cnt)
        print()


def clean_annotations(worker_pair_dict, dict_list_out, contradiction_pairs, v = False):

    worker_pairs_to_keep = set()
    worker_pairs_to_discard = set()
    worker_contradictions = defaultdict(list)
    for worker, pair_dict in worker_pair_dict.items():
        if v == True:
            print(f'checking worker: {worker}')
        for pair, relation_vec in pair_dict.items():
            relation_counts = get_relation_counts(relation_vec, normalize = True)
            contradictions = consistency_check(contradiction_pairs, relation_counts, thresh = 0.0)
            worker_contradictions[worker].extend(contradictions)
            if len(contradictions) > 0:
                if v == True:
                    print('contradiction_count', worker, pair)
                worker_pairs_to_discard.add((worker, pair))
            else:
                clean_worker_pair = (worker, pair)
                worker_pairs_to_keep.add(clean_worker_pair)
        if v == True:
            print()

    dict_list_clean = get_selected_annotations(dict_list_out, worker_pairs_to_keep)
    dict_list_discard = get_selected_annotations(dict_list_out, worker_pairs_to_discard)
    print(f'original number of annotations: {len(dict_list_out)}')
    print(f'number of clean annotations: {len(dict_list_clean)}')
    print(f'number of discarded annotations: {len(dict_list_discard)}')
    print(f'percentage discarded: {len(dict_list_discard)/len(dict_list_out)}')
    if v == True:
        print_contradiction_analysis(worker_pairs_to_discard, worker_contradictions)
    return dict_list_clean, dict_list_discard


def clean_annotations_to_file(dict_list_clean, run, group):
    dir_path = '../data/prolific_output_no_contradicting_annotations'
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)

    filepath = f'run{run}-group_{group}.csv'
    fieldnames = dict_list_clean[0].keys()
    with open(f'{dir_path}/{filepath}', 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames = fieldnames)
        writer.writeheader()
        for d in dict_list_clean:
            writer.writerow(d)

def clean_data(run, group, n_q, batch, remove_not_val= True):
    dict_list_out = load_experiment_data(run, group, n_q, batch, remove_not_val = remove_not_val)
    contradiction_pairs = load_contradiction_pairs()
    worker_pair_dict = get_worker_pair_dict(dict_list_out)
    dict_list_clean, dict_list_discard = clean_annotations(worker_pair_dict,\
                                                           dict_list_out,\
                                                           contradiction_pairs,\
                                                            v = False)
    clean_annotations_to_file(dict_list_clean, run, group)




def main():

    run = 3
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    clean_data(run, group, n_q, batch, remove_not_val= True)

if __name__ == '__main__':
    main()
