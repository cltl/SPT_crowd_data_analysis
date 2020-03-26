from load_data import load_experiment_data
from utils_analysis import load_contradiction_pairs
from utils_analysis import collect_contradictions
from utils_analysis import sort_by_key
from utils_analysis import get_annotation_ids

from collections import Counter
import pandas as pd
import os

def get_cont_type_dicts(contradictions, cont_type_cnt):
    contradiction_dict = dict()
    for cont in contradictions:
        cont = tuple(sorted(cont))
        cnt = cont_type_cnt[cont]
        cont_str = '-'.join(cont)
        contradiction_dict[cont_str] = cnt
    return contradiction_dict


def get_average_time_worker(worker_dict_list):

    data_by_batch = sort_by_key(worker_dict_list, ['filename'])
    av_time_questions = []
    for batch, dl in data_by_batch.items():
        # time info is the same for the entire batch
        time = float(dl[0]['time_taken_batch'])
        av_time_question = time / len(dl)
        av_time_questions.append(av_time_question)
    av_time = sum(av_time_questions) / len(av_time_questions)
    return av_time


def get_tests_and_checks(worker_dict_list):
    fails = []
    for d in worker_dict_list:
        quid = d['quid']
        if quid.startswith('check') or quid.startswith('test'):
            actual_answer = d['answer']
            if quid in ['check1', 'check2', 'check3']:
                correct_answer = 'true'
            elif quid.startswith('test'):
                correct_answer = d['relation'].split('_')[1]
            elif quid == 'check4':
                # if quid == check4 (I am answering questions at random)
                correct_answer = 'false'
            #check if correct
            if correct_answer != actual_answer:
                worker = d['workerid']
                fails.append(d['description'])
    return fails


def get_pair_analysis(data_dict_list, name):

    pair_data_dicts = []
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    contradictions = load_contradiction_pairs()

    for pair, dl_pair in data_by_pair.items():
        d = dict()
        n_annotations = len(dl_pair)
        data_by_worker = sort_by_key(dl_pair, ['workerid'])
        cont_cnt = Counter()
        av_time_all_workers = []
        d['pair'] = pair
        workers_with_contradictions = []
        d['n_annotations'] = n_annotations
        n_workers = len(data_by_worker)
        d['n_workers'] = n_workers
        annoation_ids_with_contradictions = []
        for worker, dl_worker in data_by_worker.items():
            av_time_all_workers.append(get_average_time_worker(dl_worker))
            pair_worker_cont = collect_contradictions(dl_worker, contradictions, threshold = 0)
            if len(pair_worker_cont) > 0:
                workers_with_contradictions.append(worker)
                # collect annotation_ids
                annoation_ids_with_contradictions.extend(get_annotation_ids(dl_worker))
            cont_cnt.update(pair_worker_cont)
        n_contradictions = sum(cont_cnt.values())
        d['n_contradictions'] = n_contradictions
        d['n_workers_contradicting'] = len(workers_with_contradictions)
        d['ratio_workers_contradicting'] = len(workers_with_contradictions)/n_workers
        d['contradiction_annotation_ratio'] = n_contradictions/n_annotations
        d['average_time_pair'] = sum(av_time_all_workers)/len(av_time_all_workers)
        d['workers_contradicting'] = ' '.join(workers_with_contradictions)
        workers_not_contradicting = [w for w in data_by_worker if w \
                                     not in workers_with_contradictions]
        d['workers_not_contradicting'] = ' '.join(workers_not_contradicting)
        # add contradiction_type analysis
        d.update(cont_cnt)
        d['annotations_with_contradiction'] = ' '.join(annoation_ids_with_contradictions)
        pair_data_dicts.append(d)

    pair_df = pd.DataFrame(pair_data_dicts)
    # sort by contradiction to annotation ratio
    pair_df.sort_values('contradiction_annotation_ratio', axis=0, ascending=False, inplace=True)
    out_dir = '../analyses/pairs/'
    os.makedirs(out_dir, exist_ok=True)
    filepath = f'{out_dir}{name}.csv'
    pair_df.to_csv(filepath, index=False)
    return pair_df, filepath

def show_pairs_of_worker(worker, df):
    print(f'Worker {worker} contradicted themselves in the following pairs:')
    print()
    for ind, row in df.iterrows():
        workers_cont = row['workers_contradicting'].split(' ')
        if worker in workers_cont:
            pair = row['pair']
            print(f'{pair} \t total workers contradicting themselves: {len(workers_cont)}')



def get_ratio_contradicting_pair_annotations(df):

    n_worker_pairs_total = 0.0
    n_worker_pairs_contradicting = 0.0

    for ind, row in df.iterrows():
        n_worker_pairs_total += row['n_workers']
        n_worker_pairs_contradicting += row['n_workers_contradicting']

    if n_worker_pairs_contradicting != 0:
        ratio = n_worker_pairs_contradicting / n_worker_pairs_total
    else:
        ratio = 0.0
    return ratio



def comparison_general(name1, name2, df1, df2):

    ratio1 = get_ratio_contradicting_pair_annotations(df1)
    ratio2 = get_ratio_contradicting_pair_annotations(df2)

    print(f'Set {name1} as a contradiction ratio of {ratio1}')
    print(f'Set {name2} as a contradiction ratio of {ratio2}')
    print('The ratio is based on the number of workers annotating a pair.')
    print('A worker always annotates a full set.')
    return ratio1, ratio2

def comparison_matching_pairs(name1, name2, df1, df2):

    # get overlapping pairs
    pairs_df1 = set([row['pair'] for ind, row in df1.iterrows()])
    pairs_df2 = set([row['pair'] for ind, row in df2.iterrows()])
    shared_pairs = pairs_df1.intersection(pairs_df2)

    rows_df1_clean = [row for ind, row in df1.iterrows() if row['pair'] in shared_pairs]
    rows_df2_clean = [row for ind, row in df2.iterrows() if row['pair'] in shared_pairs]

    df1_clean = pd.DataFrame(rows_df1_clean)
    df2_clean = pd.DataFrame(rows_df2_clean)

    #df_add_row = df_merge_col.append(add_row, ignore_index=True)
    ratio1, ratio2 = comparison_general(name1, name2, df1_clean, df2_clean)
    print(f'This analysis only includes pairs annotated in run {name1} and run {name2}.')
    return ratio1, ratio2

def compare_runs(name1, name2, df1, df2, comp = 'all'):
    if comp == 'all':
        r1, r2 = comparison_general(name1, name2, df1, df2)
    elif comp == 'pairs':
        r1, r2 = comparison_matching_pairs(name1, name2, df1, df2)
    return r1, r2


def main():
    # analyze all data:
    run = '3'
    batch = '16'
    n_q = '*'
    group = 'experiment1'

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    df, filepath = get_pair_analysis(data_dict_list, name)
    print(f'analysis can be found at: {filepath}')

    run = '1'
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    df1, filepath = get_pair_analysis(data_dict_list, name)
    print(f'analysis can be found at: {filepath}')

    run = '3'
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    df2, filepath = get_pair_analysis(data_dict_list, name)
    print(f'analysis can be found at: {filepath}')

    name1 = '1'
    name2 = '3'
    compare_runs(name1, name2, df1, df2, comp = 'all')
    print()
    compare_runs(name1, name2, df1, df2, comp = 'pairs')

if __name__ == '__main__':
    main()
