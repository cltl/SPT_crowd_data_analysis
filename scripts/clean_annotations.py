from load_data import load_experiment_data
from calculate_iaa import get_agreement

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


def remove_annotations(all_annotations, annotations_to_remove):
    annotations_removed = [d for d in all_annotations if \
                         d['uuid'] in annotations_to_remove]
    annotations_clean = [d for d in all_annotations \
                           if d['uuid'] not in annotations_to_remove]
    return annotations_clean, annotations_removed


def clean_annotations(run, group, n_q, batch, category, thresh = 0):

    all_annotations = load_experiment_data(run, group, n_q, batch, remove_not_val = True)

    if category == 'pairs_contradictions':
        dict_list_to_remove = load_analysis_data(run, group, batch, 'pairs')
        annotations_to_remove = get_annotations_to_remove(dict_list_to_remove,\
                                                      'annotations_with_contradiction')
    elif category == 'worker_contradictions':
        dict_list_to_remove = load_analysis_data(run, group, batch, 'workers')
        dict_list_to_remove = [d for d in dict_list_to_remove \
                               if int(d['n_contradictions']) > thresh]
        annotations_to_remove = get_annotations_to_remove(dict_list_to_remove,\
                                                          'annotations')

    elif category == 'worker_checks':
        dict_list_to_remove = load_analysis_data(run, group, batch, 'workers')
        dict_list_to_remove = [d for d in dict_list_to_remove \
                               if int(d['n_fails']) > thresh]
        annotations_to_remove = get_annotations_to_remove(dict_list_to_remove,\
                                                          'annotations')

    annotations_clean, annotations_removed = remove_annotations(all_annotations,\
                                                                annotations_to_remove)

    print(f'Found {len(annotations_clean)} clean annotations.')
    print(f'Round {len(annotations_to_remove)} to remove.')
    print()
    return annotations_clean, annotations_removed

def main():

    run = 3
    group = 'experiment1'
    batch = '*'
    n_q = '*'


    category = 'pairs_contradictions'
    print(f'analyze {category}')
    annotations_clean, annotations_removed = clean_annotations(run, group, n_q, batch, category)

    category = 'worker_contradictions'
    print(f'analyze {category}')
    annotations_clean, annotations_removed = clean_annotations(run, group, n_q, batch, category)

    category = 'worker_checks'
    print(f'analyze {category}')
    annotations_clean, annotations_removed = clean_annotations(run, group, n_q, batch, category)


if __name__ == '__main__':
    main()
