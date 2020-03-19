from collections import defaultdict
import csv
import pandas as pd

from utils import get_pair_dict
from utils import load_experiment_data
from utils import get_worker_pair_dict
from utils import load_contradiction_pairs

from utils import get_relation_counts
from utils import consistency_check


def get_dict_lists(list_dict, dict_list_n, k_name, v_name):
    dict_list = []
    for k, l in dict_list_n.items():
        cnt_dict = dict()
        n = len(list_dict[k])
        total = len(l)
        cnt_dict[k_name] = k
        cnt_dict[v_name] = n
        cnt_dict['total'] = total
        cnt_dict['proportion'] = n/total
        dict_list.append(cnt_dict)
    return dict_list

def dicts_to_file(dicts, name):
    fieldnames = dicts[0].keys()
    with open(name, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames = fieldnames)
        writer.writeheader()
        for d in dicts:
            writer.writerow(d)

def collect_contradictions(worker_pair_dict, dict_list_out, contradiction_pairs, v=True):

    pair_workers_contradicting = defaultdict(list)
    pair_nworkers = defaultdict(set)
    for worker, pair_dict in worker_pair_dict.items():
        for pair, relation_vec in pair_dict.items():
            relation_counts = get_relation_counts(relation_vec, normalize = True)
            contradictions = consistency_check(contradiction_pairs, relation_counts,\
                                               thresh = 0.0)
            pair_nworkers[pair].add(worker)
            if len(contradictions) > 0:
                if v == True:
                    print('contradiction_count', worker, pair)
                pair_workers_contradicting[pair].append(worker)
    dict_list_pairs = get_dict_lists(pair_workers_contradicting,\
                                     pair_nworkers, 'pair', 'pairs_removed')
    return dict_list_pairs



def contradiction_analysis(contradiction_pairs, run, group, n_q, batch, remove_not_val = True):

    dict_list_out = load_experiment_data(run, group, n_q, batch,\
                                         remove_not_val = remove_not_val)
    worker_pair_dict = get_worker_pair_dict(dict_list_out)
    dict_list_pairs = collect_contradictions(worker_pair_dict, dict_list_out,\
                                            contradiction_pairs, v = False)

    pair_cont_name = 'pairs_removed'
    filepath = f'../analyses/contradiction/{pair_cont_name}-run{run}-group_{group}.csv'
    dicts_to_file(dict_list_pairs, filepath)
    return filepath, dict_list_pairs

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
            pair_dict['pair'] = '-'.join(pair)
            pair_dict.update(analysis_dict)
            comparison_dicts.append(pair_dict)
    return comparison_dicts

def get_average_dict(run_analysis_dict, comparison_dicts):
    average_dict = dict()
    for run in run_analysis_dict.keys():
        all_values = [d[run] for d in comparison_dicts]
        average_dict[run] = sum(all_values)/len(all_values)
    return average_dict

def main():

    contradiction_pairs = load_contradiction_pairs()
    run_analysis_dict = dict()

    run = 3
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    filepath, dict_list_pairs = contradiction_analysis(contradiction_pairs,\
                                run, group, n_q, batch, remove_not_val = True)
    run_analysis_dict[run] = dict_list_pairs

    run = 1
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    filepath, dict_list_pairs = contradiction_analysis(contradiction_pairs,\
                                run, group, n_q, batch, remove_not_val = True)
    run_analysis_dict[run] = dict_list_pairs


    pair_analysis_dict = get_counts_same_pairs(run_analysis_dict, factor = 'proportion')
    comparison_dicts = get_comparison_dicts(pair_analysis_dict, run_analysis_dict)


    #print(df_pairs_compared)
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

    print()
    for run, av in average_dict.items():
        print('run', run, av)

if __name__ == '__main__':
    main()
