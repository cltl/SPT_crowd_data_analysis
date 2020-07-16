# code analyze workers


# add annotations to worker file


from utils_data import load_experiment_data, load_config
from utils_analysis import load_contradiction_pairs
from utils_analysis import collect_contradictions
from utils_analysis import sort_by_key
from utils_analysis import get_annotation_ids
from utils_analysis import get_average_time_worker, get_tests_and_checks

#from analyze_worker_outliers import get_worker_contradiction_outlier_analysis

from collections import Counter
import pandas as pd
import os
import argparse



def get_cont_type_dicts(contradictions, cont_type_cnt):
    contradiction_dict = dict()
    for cont in contradictions:
        cont = tuple(sorted(cont))
        cnt = cont_type_cnt[cont]
        cont_str = '-'.join(cont)
        contradiction_dict[cont_str] = cnt
    return contradiction_dict


def get_worker_data(data_by_worker, contradictions):
    worker_data_dicts = []
    for worker, dl_worker in data_by_worker.items():
        d = dict()
        n_annotations = len(dl_worker)
        fails = get_tests_and_checks(dl_worker)
        d['workerid'] = worker
        d['n_annotations'] = n_annotations
        cont_cnt = Counter()
        data_by_pair = sort_by_key(dl_worker, ['property', 'concept'])
        n_possible_contradictions = 0
        pairs_with_cont = 0
        for pair, dl_pair in data_by_pair.items():
            pair_contradictions = collect_contradictions(dl_pair, contradictions, threshold = 0)
            relations = [d['relation'] for d in dl_pair]
            for r1, r2 in contradictions:
                if r1 in relations and r2 in relations:
                    n_possible_contradictions += 1
            cont_cnt.update(pair_contradictions)
            if len(pair_contradictions) != 0:
                pairs_with_cont += 1
        n_contradictions = sum(cont_cnt.values())
        d['n_contradictions'] = n_contradictions
        d['n_fails'] = len(fails)
        d['contradiction_annotation_ratio'] = n_contradictions/n_annotations
        d['n_possible_contradictions'] = n_possible_contradictions
        if n_possible_contradictions != 0:
            d['contradiction_poss_contradiction_ratio'] = n_contradictions/n_possible_contradictions
        else:
            d['contradiction_poss_contradiction_ratio'] = 0
        d['fail_annotation_ratio'] = len(fails) / n_annotations
        d['contradictory_pairs_ratio'] = pairs_with_cont/len(data_by_pair)
        d['average_time_question'] = get_average_time_worker(dl_worker)
        d['annotations'] = ' '.join(get_annotation_ids(dl_worker))
        # normalize number of contradictions per type by total number of possible contradictions
        for cont, cnt in cont_cnt.items():
            if n_possible_contradictions != 0:
                cnt_norm = cnt/n_possible_contradictions
            else:
                cnt_norm = 0
            d[cont] = cnt_norm
        worker_data_dicts.append(d)
    return worker_data_dicts



def analysis_to_file(analysis_data_dicts, out_dir, name):
    df = pd.DataFrame(analysis_data_dicts)
    # sort by contradiction to annotation ratio
    df.sort_values('contradiction_poss_contradiction_ratio', axis=0, ascending=False, inplace=True)
    os.makedirs(out_dir, exist_ok=True)
    filepath = f'{out_dir}{name}.csv'
    df.to_csv(filepath, index=False)
    return df, filepath

def get_worker_analysis_total(data_dict_list, contradictions):
    data_by_worker = sort_by_key(data_dict_list, ['workerid'])
    analysis_data_dicts = get_worker_data(data_by_worker, contradictions)
    return analysis_data_dicts



def get_worker_analysis_by_batch(data_dict_list, contradictions):

    analysis_data_dicts = []
    data_by_batch = sort_by_key(data_dict_list, ['filename','completionurl'])
    for f_url, data in data_by_batch.items():
        data_by_worker = sort_by_key(data, ['workerid'])
        worker_data_dicts = get_worker_data(data_by_worker, contradictions)
        for d in worker_data_dicts:
            d['filename-url'] = f_url
        analysis_data_dicts.extend(worker_data_dicts)
    return analysis_data_dicts



def get_worker_analysis_by_pair(data_dict_list, contradictions):
    analysis_data_dicts = []
    data_by_pair = sort_by_key(data_dict_list, ['property','concept'])
    for pair, data in data_by_pair.items():
        data_by_worker = sort_by_key(data, ['workerid'])
        worker_data_dicts = get_worker_data(data_by_worker, contradictions)
        for d in worker_data_dicts:
            d['pair'] = pair
        analysis_data_dicts.extend(worker_data_dicts)
    return analysis_data_dicts


def get_worker_analysis(data_dict_list, name, unit):

    contradictions = load_contradiction_pairs()

    if unit == 'pair':
        analysis_data_dicts = get_worker_analysis_by_pair(data_dict_list, contradictions)
        out_dir = '../analyses/workers_by_pair/'
    elif unit == 'total':
        analysis_data_dicts = get_worker_analysis_total(data_dict_list, contradictions)
        out_dir = '../analyses/workers/'
    elif unit == 'batch':
        analysis_data_dicts = get_worker_analysis_by_batch(data_dict_list, contradictions)
        out_dir = '../analyses/workers_by_batch/'
    worker_df, filepath = analysis_to_file(analysis_data_dicts, out_dir, name)
    return worker_df, filepath



def main():

    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']

    parser = argparse.ArgumentParser()
    parser.add_argument("--units", default=['total', 'pair', 'batch'], type=list, nargs="+")
    args = parser.parse_args()
    units  = args.units

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')

    for unit in units:
        print(f'analyzing workers on the level of: {unit}')
        df, filepath = get_worker_analysis(data_dict_list, name, unit)
        print(f'analysis can be found at: {filepath}')



if __name__ == '__main__':
    main()
