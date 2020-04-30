from load_data import load_experiment_data
from utils_analysis import sort_by_key
from calculate_iaa import get_full_report
from calculate_iaa import load_rel_level_mapping
from clean_annotations import filter_annotations

from collections import Counter
from collections import defaultdict
import pandas as pd
import os


def get_cont_stats(data_dict_list, aggregated_dicts, v=False):
    total_annotations = len(data_dict_list)
    total_pairs = len(aggregated_dicts)
    pairs_cont = [d for d in aggregated_dicts if d['contradiction'] == 'yes']
    pairs_no_cont = [d for d in aggregated_dicts if d['contradiction'] == 'no']
    pairs_cont_prop = len(pairs_cont)/total_pairs
    pairs_no_cont_prop = len(pairs_no_cont)/total_pairs
    if v == True:
        print(f'Total number of annotations: {total_annotations}')
        print(f'Total number of pairs: {total_pairs}')
        print(f'Number of pairs without contradictions: {len(pairs_no_cont)} ({pairs_no_cont_prop})')
        print(f'Number of pairs with contradictions: {len(pairs_cont)} ({pairs_cont_prop})')
    return pairs_cont, pairs_no_cont


def get_cont_no_cont_annotations(data_dict_list, pairs_no_cont, pairs_cont):

    annotations_no_cont = []
    annotations_cont = []
    cont = [d['pair'] for  d in pairs_cont]
    no_cont = [d['pair'] for  d in pairs_no_cont]

    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])

    for p, data_dicts in data_by_pair.items():
        if p in no_cont:
            annotations_no_cont.extend(data_dicts)
        else:
            annotations_cont.extend(data_dicts)
    return annotations_cont, annotations_no_cont

def aggregate_labels(data_dict_list):
    n = 0
    contradiction = set(['all', 'few'])
    rel_level_mapping = load_rel_level_mapping(mapping = 'levels')
    rel_header = rel_level_mapping.keys()
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    aggregated_dicts = []
    for pair, data_dicts in data_by_pair.items():
        n += 1
        pair_d = dict()
        pair_d['pair'] = pair
        data_by_rel = sort_by_key(data_dicts, ['relation'])
        levels = set()
        majority_labels = []
        most_votes_labels = defaultdict(list)
        for rel  in rel_header:
            rel_dicts = data_by_rel[rel]
            n_annotations = len(rel_dicts)
            n_true = len([d['answer'] for d in rel_dicts if d['answer'] == 'true'])
            if n_true > 0:
                prop_true = n_true/n_annotations
            else:
                prop_true = 0.0
            if prop_true > 0.5:
                majority_labels.append(rel)
                levels.add(rel_level_mapping[rel])
            most_votes_labels[prop_true].append(rel)
        if levels == contradiction:
            pair_d['contradiction'] = 'yes'
        elif len(levels) == 1:
            pair_d['contradiction'] = 'no'
        else:
            pair_d['contradiction'] = 'no'
        pair_d['levels'] = '-'.join(sorted(list(levels)))
        pair_d['majority_labels'] = '-'.join(majority_labels)
        n_most_votes = max(list(most_votes_labels.keys()))
        pair_d['label(s)_most_vostes'] = '-'.join(most_votes_labels[n_most_votes])
        pair_d['proportion_most_votes'] = n_most_votes
        aggregated_dicts.append(pair_d)
    return aggregated_dicts




def get_aggregated_data(run, batch, n_q, group, annotation_filter, iaa=True, v=False):

    annotations_clean, annotations_removed = filter_annotations(\
                                            run, group, n_q, batch,\
                                            annotation_filter, iaa=True)
    agg_labels_all = aggregate_labels(annotations_clean)
    pairs_cont, pairs_no_cont = get_cont_stats(annotations_clean, agg_labels_all)
    annotations_cont, annotations_no_cont = get_cont_no_cont_annotations(\
                                            annotations_clean, pairs_no_cont, pairs_cont)
    dir_path = f'../aggregated_labels/annotation_filter-{annotation_filter}'
    os.makedirs(dir_path, exist_ok=True)
    filepath = f'{dir_path}/aggregated_labels-run{run}-group_{group}.csv'
    filepath = filepath.replace('*', 'all')
    all_df = pd.DataFrame(agg_labels_all)
    all_df.to_csv(filepath, index=False)
    #print()
    print(f'Aggregated labels written to: {filepath}')

    iaa_dict = get_full_report(annotations_clean)
    percent_clean = len(annotations_clean)/(len(annotations_clean) + len(annotations_removed))
    if v == True:
        print(f'Percent of clean annotations: {round(percent_clean*100, 2)}%')
    stats_dict = dict()
    stats_dict['filter'] = annotation_filter
    stats_dict['percent_clean'] = percent_clean
    stats_dict['Alpha_full'] = iaa_dict['full']['Krippendorff']
    stats_dict['Alpha_levels'] = iaa_dict['levels']['Krippendorff']
    stats_dict['Kappa_full'] = iaa_dict['full']['Av_Cohens_kappa']
    stats_dict['Kappa_levels'] = iaa_dict['levels']['Av_Cohens_kappa']
    stats_dict['percent_pairs_cont'] = len(pairs_cont)/(len(pairs_cont)+len(pairs_no_cont))
    return all_df, stats_dict

def overview_to_file(stats_df, run, group):
    stats_dir = '../aggregated_labels/stats_overviews/'
    os.makedirs(stats_dir, exist_ok=True)
    stats_path = f'{stats_dir}run{run}-group_{group}.csv'
    stats_df.to_csv(stats_path)

def get_overview(run, batch, n_q, group):
    filters = [
        'contradiction_outliers',
        'worker_contradiction_rate_0',
        'worker_contradiction_rate_above_av',
        'worker_failed_checks_1',
        'none',
            ]
    stats_dicts = []
    for annotation_filter in filters:
        all_df, stats_dict = get_aggregated_data(run, batch, n_q, group, annotation_filter)
        stats_dicts.append(stats_dict)
    stats_df = pd.DataFrame(stats_dicts)
    overview_to_file(stats_df, run, group)
    return stats_df


def main():
    run = '4'
    batch = '*'
    n_q = '*'
    group = 'experiment2'

    stats_df = get_overview(run, batch, n_q, group)





if __name__ == '__main__':
    main()
