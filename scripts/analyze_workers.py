from load_data import load_experiment_data

from collections import defaultdict
from collections import Counter
import pandas as pd
import os

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
        if d['answer'] == 'true':
            val = 1
        else:
            val = 0
        relation_cnt[d['relation']] += val

    return relation_cnt

def get_relation_pairs(pair_dicts, threshold = 0):
    relation_cnt = get_relation_cnt(pair_dicts)
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
    contradiction_pairs = [tuple(sorted(p)) for p in relation_pairs if p in contradictions]
    return contradiction_pairs


def load_contradiction_pairs():
    contradictions = []
    with open('../scheme/contradictions.csv') as infile:
        for line in infile:
            contradictions.append(set(line.strip('\n').split(',')))
    return contradictions

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

def get_worker_analysis(data_dict_list, name):

    worker_data_dicts = []
    data_by_worker = sort_by_key(data_dict_list, ['workerid'])
    contradictions = load_contradiction_pairs()

    for worker, dl_worker in data_by_worker.items():
        d = dict()
        n_annotations = len(dl_worker)
        fails = get_tests_and_checks(dl_worker)
        d['workerid'] = worker
        d['n_annotations'] = n_annotations

        cont_cnt = Counter()
        data_by_pair = sort_by_key(dl_worker, ['property', 'concept'])
        for pair, dl_pair in data_by_pair.items():
            pair_contradictions = collect_contradictions(dl_pair, contradictions, threshold = 0)
            cont_cnt.update(pair_contradictions)
        n_contradictions = sum(cont_cnt.values())
        d['n_contradictions'] = n_contradictions
        d['n_fails'] = len(fails)
        d['contradiction_annotation_ratio'] = n_contradictions/n_annotations
        d['fail_annotation_ratio'] = len(fails) / n_annotations
        d['average_time_question'] = get_average_time_worker(dl_worker)
        # add contradiction_type analysis
        d.update(cont_cnt)
        worker_data_dicts.append(d)

    worker_df = pd.DataFrame(worker_data_dicts)
    # sort by contradiction to annotation ratio
    worker_df.sort_values('contradiction_annotation_ratio', axis=0, ascending=False, inplace=True)
    out_dir = '../analyses/workers/'
    os.makedirs(out_dir, exist_ok=True)
    filepath = f'{out_dir}{name}.csv'
    worker_df.to_csv(filepath)
    return worker_df, filepath


def main():
    # analyze all data:
    run = '1'
    batch = '*'
    n_q = '*'
    group = 'experiment1'

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    df, filepath = get_worker_analysis(data_dict_list, name)
    print(f'analysis can be found at: {filepath}')

if __name__ == '__main__':
    main()
