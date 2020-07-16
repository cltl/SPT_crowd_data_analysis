# Pair analysis

from utils_data import load_experiment_data, load_config
from utils_analysis import load_contradiction_pairs
from utils_analysis import collect_contradictions
from utils_analysis import sort_by_key
from utils_analysis import get_annotation_ids
from utils_analysis import get_average_time_worker, get_tests_and_checks

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
        n_possible_contradictions = 0
        for worker, dl_worker in data_by_worker.items():
            av_time_all_workers.append(get_average_time_worker(dl_worker))
            pair_worker_cont = collect_contradictions(dl_worker, contradictions, threshold = 0)
            relations = [d['relation'] for d in dl_worker]
            for r1, r2 in contradictions:
                if r1 in relations and r2 in relations:
                    n_possible_contradictions += 1
            if len(pair_worker_cont) > 0:
                workers_with_contradictions.append(worker)
                annoation_ids_with_contradictions.extend(get_annotation_ids(dl_worker))
            cont_cnt.update(pair_worker_cont)
        n_contradictions = sum(cont_cnt.values())
        d['n_contradictions'] = n_contradictions
        d['n_workers_contradicting'] = len(workers_with_contradictions)
        d['ratio_workers_contradicting'] = len(workers_with_contradictions)/n_workers
        d['contradiction_annotation_ratio'] = n_contradictions/n_annotations
        d['n_possible_contradictions'] = n_possible_contradictions
        if n_possible_contradictions != 0:
            d['contradiction_poss_contradiction_ratio'] = n_contradictions/n_possible_contradictions
        else:
            d['contradiction_poss_contradiction_ratio'] = 0
        d['average_time_pair'] = sum(av_time_all_workers)/len(av_time_all_workers)
        d['workers_contradicting'] = ' '.join(workers_with_contradictions)
        workers_not_contradicting = [w for w in data_by_worker if w \
                                     not in workers_with_contradictions]
        d['workers_not_contradicting'] = ' '.join(workers_not_contradicting)
        d.update(cont_cnt)
        d['annotations_with_contradiction'] = ' '.join(annoation_ids_with_contradictions)
        pair_data_dicts.append(d)

    pair_df = pd.DataFrame(pair_data_dicts)
    # sort by contradiction to annotation ratio
    pair_df.sort_values('ratio_workers_contradicting', axis=0, ascending=False, inplace=True)
    out_dir = '../analyses/pairs/'
    os.makedirs(out_dir, exist_ok=True)
    filepath = f'{out_dir}{name}.csv'
    pair_df.to_csv(filepath, index=False)
    return pair_df, filepath



def main():

    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    df, filepath = get_pair_analysis(data_dict_list, name)
    print(f'analysis can be found at: {filepath}')



if __name__ == '__main__':
    main()
