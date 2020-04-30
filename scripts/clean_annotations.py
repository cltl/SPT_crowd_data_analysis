from load_data import load_experiment_data
from calculate_iaa import get_full_report
from utils_analysis import sort_by_key

import csv

def load_analysis_data(run, group, batch, category):
    dir_path = f'../analyses/{category}/'
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
    path = f'{dir_path}{name}.csv'
    with open(path) as infile:
        dict_list = list(csv.DictReader(infile))
    return dict_list

def get_annotations_to_remove(dict_list, key):
    annotations_to_remove = []
    for d in dict_list:
        uuids = d[key].split(' ')
        annotations_to_remove.extend(uuids)
    return annotations_to_remove


def remove_annotations(all_annotations, annotations_to_remove, v=False):
    annotations_removed = [d for d in all_annotations if \
                         d['uuid'] in annotations_to_remove]
    annotations_clean = [d for d in all_annotations \
                           if d['uuid'] not in annotations_to_remove]

    if v == True:
        print('----Filter report----')
        print(f'Total number of annotations: {len(all_annotations)}')
        print(f'Number of clean annotations: {len(annotations_clean)}')
        print(f'Number of removed annotations: {len(annotations_removed)}')
        print('---------------------')
        print()
    return annotations_clean, annotations_removed

def get_avergage_cont_rate(dict_list_workers):
    sum_cont_rate = 0.0
    for d in dict_list_workers:
        cont_rate = d['contradiction_poss_contradiction_ratio']
        sum_cont_rate += float(cont_rate)
    av_cont_rate = sum_cont_rate/len(dict_list_workers)
    return av_cont_rate


def get_worker_outliers(outlier_overview):
    worker_outliers = dict()
    for d in outlier_overview:
        worker = d['workerid']
        outliers = d['outlier_contradictions']
        outliers_list = outliers.split(') (')
        outlier_pairs = []
        for out in outliers_list:
            out_tuple = tuple(out.replace("('", '').replace("')", '').replace("'", '').split(', '))
            outlier_pairs.append(out_tuple)
        worker_outliers[worker] = outlier_pairs
    return worker_outliers

def collect_outlier_annotations(worker_outliers, all_annotations):
    annotations_to_remove = []
    annotations_clean = []
    annotations_by_worker = sort_by_key(all_annotations, ['workerid'])
    for worker, annotations_w in annotations_by_worker.items():
        outliers = worker_outliers[worker]
        annotations_by_pair = sort_by_key(annotations_w, ['property', 'concept'])
        for p, annotations in annotations_by_pair.items():
            relations_true = [d['relation'] for d in annotations if d['answer'] == 'true']
            for out_pair in outliers:
                if all([out in relations_true for out in out_pair]):
                    uuids = [d['uuid'] for d in annotations]
                    annotations_to_remove.extend(uuids)

    return annotations_to_remove


def remove_workers_cont(run, group, n_q, batch, thresh = 'av_cont_rate'):
    all_annotations = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    dict_list_workers = load_analysis_data(run, group, batch, 'workers')
    if thresh == 'av_cont_rate':
        av = get_avergage_cont_rate(dict_list_workers)
        dict_list_workers_to_remove = [d for d in dict_list_workers \
                               if float(d['contradiction_poss_contradiction_ratio']) > av]
    else:
        dict_list_workers_to_remove = [d for d in dict_list_workers \
                               if float(d['contradiction_poss_contradiction_ratio']) > thresh]
    annotations_to_remove = get_annotations_to_remove(dict_list_workers_to_remove,\
                                                      'annotations')
    annotations_clean, annotations_removed = remove_annotations(all_annotations,\
                                                                annotations_to_remove)
    return annotations_clean, annotations_removed


def remove_workers_check(run, group, n_q, batch, thresh):
    all_annotations = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    dict_list_workers = load_analysis_data(run, group, batch, 'workers')
    dict_list_workers_to_remove = [d for d in dict_list_workers \
                               if float(d['n_fails']) > thresh]
    annotations_to_remove = get_annotations_to_remove(dict_list_workers_to_remove,\
                                                      'annotations')
    annotations_clean, annotations_removed = remove_annotations(all_annotations,\
                                                                annotations_to_remove)
    return annotations_clean, annotations_removed



def remove_outlier_annotations(run, group, n_q, batch):
    all_annotations = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    outlier_overview = load_analysis_data(run, group, batch, 'workers-outliers')
    worker_outliers = get_worker_outliers(outlier_overview)
    annotations_to_remove = collect_outlier_annotations(\
                                                worker_outliers, all_annotations)
    annotations_clean, annotations_removed =remove_annotations(all_annotations,\
                                                               annotations_to_remove)
    return annotations_clean, annotations_removed


def filter_annotations(run, group, n_q, batch, annotation_filter, iaa=False, v=False):
    if v == True:
        print(f'\nFiltering out {annotation_filter}\n')

    if annotation_filter == 'contradiction_outliers':
        annotations_clean, annotations_removed = remove_outlier_annotations(\
                                                run, group, n_q, batch)
    elif annotation_filter == 'worker_contradiction_rate_0':
        annotations_clean, annotations_removed = remove_workers_cont(run, group, \
                                                            n_q, batch, \
                                                            thresh = 0.0)
    elif annotation_filter == 'worker_contradiction_rate_above_av':
        annotations_clean, annotations_removed = remove_workers_cont(run, group, \
                                                            n_q, batch, \
                                                            thresh = 'av_cont_rate')
    elif annotation_filter == 'worker_failed_checks_1':
        annotations_clean, annotations_removed  = remove_workers_check(run, \
                                                                       group, n_q, batch, 1)
    elif annotation_filter == 'none':
        annotations_clean = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
        annotations_removed = []

    return annotations_clean, annotations_removed



def main():

    run = '4'
    group = 'experiment2'
    batch = '*'
    n_q = '*'

    annotation_filter = 'none'
    annotations_clean, annotations_removed = filter_annotations(run, group, n_q, batch, annotation_filter, iaa=True)
    full_ag_dict = get_full_report(annotations_clean)

    annotation_filter = 'contradiction_outliers'
    annotations_clean, annotations_removed = filter_annotations(run, group, n_q, batch, annotation_filter, iaa=True)

    annotation_filter = 'worker_contradiction_rate_0'
    annotations_clean, annotations_removed = filter_annotations(run, group, n_q, batch, annotation_filter, iaa=True)

if __name__ == '__main__':
    main()
