from collections import defaultdict
from collections import Counter
import csv
import pandas as pd

from utils import get_pair_dict
from utils import load_experiment_data
from utils import get_worker_pair_dict
from utils import load_contradiction_pairs

from utils import get_relation_counts
from utils import consistency_check



def dicts_to_file(dicts, name):
    fieldnames = dicts[0].keys()
    with open(name, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames = fieldnames)
        writer.writeheader()
        for d in dicts:
            writer.writerow(d)



def get_dict_lists(list_dict, dict_list_n, k_name, v_name):
    dict_list = []
    for k, l in dict_list_n.items():
        cnt_dict = dict()
        n = len(list_dict[k])
        total = len(l)
        cnt_dict[k_name] = '-'.join(k)
        cnt_dict[v_name] = n
        cnt_dict['total'] = total
        cnt_dict['proportion'] = n/total
        cnt_dict['n removed'] = n
        cnt_dict['worker_ids'] = ' '.join(list_dict[k])
        dict_list.append(cnt_dict)
    return dict_list


def get_counts_same_pairs(run_analysis_dict, factor = 'proportion'):

    pair_analysis_dict = defaultdict(dict)
    runs = run_analysis_dict.keys()
    for run, analysis_dicts in run_analysis_dict.items():
        for analysis_dict in analysis_dicts:
            pair = analysis_dict['pair']
            pair_analysis_dict[pair][run] = analysis_dict[factor]

    return pair_analysis_dict

def get_comparison_dicts(pair_analysis_dict, run_analysis_dict):
    comparison_dicts = []
    for pair, analysis_dict in pair_analysis_dict.items():
        pair_dict = dict()
        if len(analysis_dict) == len(run_analysis_dict):
            pair_dict['pair'] = pair
            pair_dict.update(analysis_dict)
            comparison_dicts.append(pair_dict)
    return comparison_dicts

def get_average_dict(run_analysis_dict, comparison_dicts):
    average_dict = dict()
    for run in run_analysis_dict.keys():
        all_values = [d[run] for d in comparison_dicts]
        average_dict[run] = sum(all_values)/len(all_values)
    return average_dict


def get_worker_contradiction_cnt(worker_contradictions):
    worker_contradiction_cnt = Counter()
    for worker, contradictions in worker_contradictions.items():
        contradiction_counter = Counter()
        worker_contradiction_cnt[worker] += len(contradictions)
    return worker_contradiction_cnt

def get_contradiction_counts(worker_contradictions):

    contradiction_cnt = Counter()
    for w, contradictions in worker_contradictions.items():
        for c in contradictions:
            contradiction_cnt[tuple(c)] += 1
    return contradiction_cnt


def worker_contradictions_sorted(worker_contradiction_cnt):
    dict_list = []
    for w, cnt in worker_contradiction_cnt.most_common():
        d = dict()
        d['worker'] = w
        d['contradiction_cnt'] = cnt
        dict_list.append(d)
    return dict_list


def collect_contradictions(worker_pair_dict, dict_list_out, contradiction_pairs, v=False):

    pair_workers_contradicting = defaultdict(list)
    pair_nworkers = defaultdict(set)
    worker_contradictions = defaultdict(list)
    pairs_with_contradiction = 0
    pair_annotations = 0
    for worker, pair_dict in worker_pair_dict.items():
        for pair, relation_vec in pair_dict.items():
            relation_counts = get_relation_counts(relation_vec, normalize = True)
            contradictions = consistency_check(contradiction_pairs, relation_counts,\
                                               thresh = 0.0)
            pair_annotations += 1
            pair_nworkers[pair].add(worker)
            worker_contradictions[worker].extend(contradictions)
            if len(contradictions) > 0:
                pairs_with_contradiction += 1
                if v == True:
                    print('contradiction_count', worker, pair)
                pair_workers_contradicting[pair].append(worker)
    dict_list_pairs = get_dict_lists(pair_workers_contradicting,\
                                     pair_nworkers, 'pair', 'pairs_removed')
    worker_contradiction_cnt = get_worker_contradiction_cnt(worker_contradictions)
    contradiction_cnt = get_contradiction_counts(worker_contradictions)
    print('Percentage of contradictory pair annotations of total pair annotations:')
    print(round((pairs_with_contradiction/pair_annotations) *100, 2))
    print('most common contradictions')
    for c, cnt in contradiction_cnt.most_common(3):
        print(c, cnt)
    return dict_list_pairs, worker_contradiction_cnt


def contradiction_analysis(contradiction_pairs, run, group, n_q, batch, remove_not_val = True):

    dict_list_out = load_experiment_data(run, group, n_q, batch,\
                                         remove_not_val = remove_not_val)
    worker_pair_dict = get_worker_pair_dict(dict_list_out)
    dict_list_pairs, worker_contradiction_cnt = collect_contradictions(worker_pair_dict, dict_list_out,\
                                            contradiction_pairs, v = False)

    dict_list_worker_contradictions = worker_contradictions_sorted(worker_contradiction_cnt)
    worker_contradictions_df = pd.DataFrame(dict_list_worker_contradictions)
    dir_path = '../analyses/contradiction/'
    pair_cont_name = 'pairs_removed'
    filepath_pairs = f'{dir_path}{pair_cont_name}-run{run}-group_{group}-batch{batch}.csv'
    dicts_to_file(dict_list_pairs, filepath_pairs)
    worker_cont_name = 'worker_contradictions'
    filepath_cont = f'{dir_path}{worker_cont_name}-run{run}-group_{group}-batch{batch}.csv'
    worker_contradictions_df.to_csv(filepath_cont)
    return filepath_pairs, filepath_cont


def run_comparison(runs, group):

    contradiction_pairs = load_contradiction_pairs()
    run_analysis_dict = dict()

    batch = '*'
    n_q = '*'

    for run in runs:
        filepath_pairs, filepath_cont  = contradiction_analysis(contradiction_pairs,\
                                        run, group,n_q, batch, remove_not_val = True)
        run_df = pd.read_csv(filepath_pairs)
        run_analysis_dict[run] = run_df.to_dict('records')
    pair_analysis_dict = get_counts_same_pairs(run_analysis_dict, factor = 'proportion')
    comparison_dicts = get_comparison_dicts(pair_analysis_dict, run_analysis_dict)

    runs = '-'.join([str(run) for run in run_analysis_dict.keys()])
    analysis_path = f'../analyses/contradiction/comparison_pairs_removed-runs{runs}.csv'

    average_dict = get_average_dict(run_analysis_dict, comparison_dicts)
    average_dict_row = dict()
    average_dict_row['pair'] = 'average'
    for run, av in average_dict.items():
        average_dict_row[run] = av

    comparison_dicts.append(average_dict_row)
    df_pairs_compared = pd.DataFrame(comparison_dicts)
    df_pairs_compared.to_csv(analysis_path)


def load_worker_cont_counts(filepath_cont):

    worker_cont_dict = Counter()
    df_cont = pd.read_csv(filepath_cont)
    cont_dicts = df_cont.to_dict('records')
    for d in cont_dicts:
        w = d['worker']
        cont = d['contradiction_cnt']
        worker_cont_dict[w] = cont
    return worker_cont_dict


def analyze_workers_batch(run, group, batch, n_q):

    # load results of current batch
    contradiction_pairs = load_contradiction_pairs()
    print(f'analyzing batch {batch}')
    filepath_pairs_batch, filepath_cont_batch = contradiction_analysis(contradiction_pairs,\
                                                run, group, n_q, batch,\
                                                remove_not_val = True)

    # load all_results
    run = '*'
    batch = '*'
    n_q = '*'
    group = 'experiment1'
    print()
    print(f'analyzing batch all annotations')
    filepath_pairs_all, filepath_cont_all = contradiction_analysis(contradiction_pairs,\
                                run, group, n_q, batch, remove_not_val = True)


    worker_cont_dict_batch = load_worker_cont_counts(filepath_cont_batch)
    worker_cont_dict_all = load_worker_cont_counts(filepath_cont_all)

    len_name_dir = len('../analyses/contradiction/pairs_removed-')
    name_batch = filepath_pairs_batch[len_name_dir:]
    worker_most_batch, cont_batch = worker_cont_dict_batch.most_common(1)[0]
    worker_most_all, cont_all = worker_cont_dict_all.most_common(1)[0]
    print()
    print(f'worker with most contradictions on batch {name_batch}:')
    print(f'worker: {worker_most_batch}')
    print(f'number of contradictions: {cont_batch}')

    if worker_most_batch in worker_cont_dict_all:
        worker_n_cont_all = worker_cont_dict_all[worker_most_batch]
    else:
        worker_n_cont_all = 0
    print(f'number of all contradictions of {worker_most_batch}: {worker_n_cont_all}')
    print(f'highest number of contradictions by a worker: {cont_all}')
    print(f'worker with most contradictions in total: {worker_most_all}')


def main():

    runs = [1, 3]
    group = 'experiment1'
    run_comparison(runs, group)

    run = 3
    batch = 6
    n_q = 70
    group = 'experiment1'
    analyze_workers_batch(run, group, batch, n_q)

if __name__ == '__main__':
    main()
