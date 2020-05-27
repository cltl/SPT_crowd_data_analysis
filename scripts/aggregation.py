from load_data import load_experiment_data
from utils_analysis import sort_by_key
from utils_analysis import load_analysis
from calculate_iaa import load_rel_level_mapping
from clean_annotations import remove_contradicting_workers
from collections import defaultdict, Counter
import pandas as pd
import os



def aggregate_binary_labels(data_dict_list):

    contradiction = set(['all', 'few'])
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    aggregated_binary_labels = []
    for pair, data_dicts in data_by_pair.items():
        if not pair.startswith('_'):
            data_by_rel = sort_by_key(data_dicts, ['relation'])
            prop_rels = defaultdict(list)
            triple_dicts = []
            for rel, data in data_by_rel.items():
                answers = [d['answer'] for d in data]
                true_cnt = answers.count('true')
                prop = true_cnt/len(answers)
                prop_rels[prop].append(rel)
                majority_vote = False
                if prop > 0.5:
                    majority_vote = True
                triple_dict = dict()
                triple_dict['relation'] = rel
                triple_dict['pair'] = pair
                triple_dict['majority_vote'] = majority_vote
                triple_dicts.append(triple_dict)
            # add top label
            top_prop = max(prop_rels.keys())

            for d in triple_dicts:
                rel = d['relation']
                if rel in prop_rels[top_prop]:
                    d['top_vote'] = True
                else:
                    d['top_vote'] = False
            aggregated_binary_labels.extend(triple_dicts)
    return aggregated_binary_labels

def aggregate_labels(data_dict_list):

    rel_level_mapping = load_rel_level_mapping(mapping = 'levels')
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])

    aggregated_labels = []
    for pair, data_dicts in data_by_pair.items():
        if not pair.startswith('_'):
            data_by_rel = sort_by_key(data_dicts, ['relation'])
            prop_rels = defaultdict(list)
            majority_labels = []
            for rel, data in data_by_rel.items():
                answers = [d['answer'] for d in data]
                true_cnt = answers.count('true')
                prop = true_cnt/len(answers)
                prop_rels[prop].append(rel)
                if prop > 0.5:
                    majority_labels.append(rel)
            top_prop = max(prop_rels.keys())
            top_labels = prop_rels[top_prop]
            top_levels = set([rel_level_mapping[l] for l in top_labels])
            majority_levels = set([rel_level_mapping[l] for l in majority_labels])
            pair_d = dict()
            pair_d['pair'] = pair
            pair_d['top_labels'] = '-'.join(sorted(top_labels))
            pair_d['majority_labels'] = '-'.join(sorted(majority_labels))
            pair_d['top_levels'] = '-'.join(sorted(top_levels))
            pair_d['majority_levels'] = '-'.join(sorted(majority_levels))
            pair_d
            pair_d['label_proportion'] = top_prop
            if 'all' in top_levels and 'few' in top_levels:
                pair_d['contradiction'] = True
            else:
                pair_d['contradiction'] = False
            aggregated_labels.append(pair_d)
    return aggregated_labels



def main():
    run = '4'
    batch = '*'
    n_q = '*'
    group = 'experiment2'

    # Total without filter
    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    data_filter = 'None'
    data_dict_list_clean = data_dict_list
    aggregated_labels = aggregate_labels(data_dict_list_clean)
    df = pd.DataFrame(aggregated_labels)
    name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')

    label_dir = f'../aggregated_labels/{data_filter}/'
    os.makedirs(label_dir, exist_ok=True)
    path = f'{label_dir}{name}.csv'
    df.to_csv(path)

    # columns to add:
    # iaa pair
    # iaa top labels
    # contradiction rate pair
    # crowd_truth pair rating


    units = ['total', 'batch', 'pair']
    stds = [0.5, 1, 1.5, 2, 2.5, 3]


    for unit in units:
        for n_stds in stds:
            if unit == 'total':
                analysis_type = 'workers'
            else:
                analysis_type = f'workers_by_{unit}'
            dict_list_workers = load_analysis(analysis_type, run, group, batch, as_dict = True)

            data_filter = f'{unit}-{n_stds}'
            dict_list_workers = load_analysis(analysis_type, run, group, batch, as_dict = True)
            data_dict_list_clean = remove_contradicting_workers(data_dict_list, dict_list_workers, unit,  n_stds)

            aggregated_labels = get_top_labels(data_dict_list_clean)
            df = pd.DataFrame(aggregated_labels)
            name = f'run{run}-group_{group}-batch{batch}'.replace('*', '-all-')
            label_dir = f'../aggregated_labels/{data_filter}/'
            os.makedirs(label_dir, exist_ok=True)
            path = f'{label_dir}{name}.csv'
            df.to_csv(path)


if __name__ == '__main__':
    main()
