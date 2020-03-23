import csv
from collections import defaultdict
from collections import Counter
from utils import load_experiment_data_batch
from utils import parse_answer
from utils import load_experiment_data
import pandas as pd


def get_tests_and_checks(dict_list):
    worker_fail_dict = defaultdict(list)
    for d in dict_list:
        quid = d['quid']
        if quid.startswith('check') or quid.startswith('test'):
            actual_answer = parse_answer(d['answer'])
            if quid in ['check1', 'check2', 'check3']:
                correct_answer = 'true'
            elif quid.startswith('test'):
                correct_answer = d['triple'].split('-')[0].split('_')[1]
            else:
                # if quid == check4 (I am answering questions at random)
                correct_answer = 'false'
            #check if correct
            if correct_answer != actual_answer:
                worker = d['workerid']
                worker_fail_dict[worker].append(d['description'])
    return worker_fail_dict


def get_fail_dicts(worker_fail_dict):
    fail_dicts = []
    for worker, fails in worker_fail_dict.items():
        f_dict = dict()
        f_dict['worker'] = worker
        f_dict['number of fails'] = len(fails)
        f_dict['types of fails'] = len(set(fails))
        fail_dicts.append(f_dict )
        #print(f'{worker} - number of fails: {len(fails)}, types of fails: {len(set(fails))}')
    return fail_dicts


def get_fail_overview(worker_fail_dict):
    fail_cnts = Counter()
    for worker, fails in worker_fail_dict.items():
        for fail in fails:
            fail_cnts[fail] += 1

    fail_overview_dicts = []
    for f, cnt in fail_cnts.most_common():
        f_dict = dict()
        f_dict['question'] = f
        f_dict['number of fails'] = cnt
        fail_overview_dicts.append(f_dict)
    return fail_overview_dicts

def sort_workers_most_fails(worker_fail_dict):
    w_cnts = Counter()
    for w, fails in worker_fail_dict.items():
        w_cnts[w] += len(fails)
    return w_cnts



def analysis_all_annotations(group):

    run = '*'
    batch = '*'
    n_q = '*'
    dir_path = '../analyses/checks/'
    file_path = f'worker_fails-group_{group}-so_far.csv'
    dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    print(f'Annotations collected so far: {len(dict_list)}')
    worker_fail_dict = get_tests_and_checks(dict_list)

    fail_dicts = get_fail_dicts(worker_fail_dict)
    fail_df = pd.DataFrame(fail_dicts)
    fail_df.to_csv(f'{dir_path}{file_path}')
    file_path_overview = 'fail_types-overviews-so_far.csv'
    fail_overview_dicts = get_fail_overview(worker_fail_dict)
    overview_df = pd.DataFrame(fail_overview_dicts)
    overview_df.to_csv(f'{dir_path}{file_path_overview}')
    worker_fail_cnts = sort_workers_most_fails(worker_fail_dict)
    #print('Most fails so far:')
    #for w, cnt in worker_fail_cnts.most_common():
     #   print(w, cnt)
    print(f'highest number of fails: {worker_fail_cnts.most_common(1)}')
    print()
    print(f'find general analysis here: {dir_path}{file_path}')
    print(f'find check analysis overview here: {dir_path}{file_path_overview}')
    return worker_fail_dict


def analysis_current_batch(run, batch, n_q, group):
    dir_path = '../analyses/checks/'
    file_path = f'worker_fails-group_{group}-run{run}-batch{batch}.csv'
    dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    worker_fail_dict = get_tests_and_checks(dict_list)
    print(f'Annotations in batch {batch}: {len(dict_list)}')
    print(f'Workers with fails in batch {batch}: {len(worker_fail_dict)}')
    for w, fails in worker_fail_dict.items():
        print(f'{w} - number of fails: {len(fails)} - types of fails {len(set(fails))}')
    print()
    fail_dicts = get_fail_dicts(worker_fail_dict)
    fail_df = pd.DataFrame(fail_dicts)
    analysis_path = f'{dir_path}{file_path}'
    fail_df.to_csv(analysis_path)
    print(f'find batch analysis here: {analysis_path}')
    return worker_fail_dict


def analyze_batch_checks(run, batch, n_q, group):

    worker_fail_dict = analysis_current_batch(run, batch, n_q, group)
    for w, fails in worker_fail_dict.items():
        print(w)
        for f in fails:
            print(f)
    worker_fail_dict_total = analysis_all_annotations(group)
    print()
    for w, f in worker_fail_dict.items():
        total_fails = worker_fail_dict_total[w]
        print(f'worker {w} has a total of {len(total_fails)} fails')

def main():
    run = 3
    batch = 6
    n_q = 70
    group = 'experiment1'
    analyze_batch_checks(run, batch, n_q, group)


if __name__ == '__main__':
    main()
