

# aggretation dev
from load_data import load_experiment_data
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from calculate_iaa import get_collapsed_relations
from clean_annotations import remove_contradicting_workers
from collections import defaultdict, Counter
import pandas as pd
import os


def split_score(ua_score):

    scores = ua_score.replace('Counter({', '').replace('})', '').split(', ')
    score_dict = dict()
    for s in scores:
        l, s = s.split(': ')
        l = l.strip("'")
        score_dict[l.strip()] = float(s)

    return score_dict

def get_ua_score(quid, units_by_quid):
    if quid in units_by_quid:
        ct_unit_d = units_by_quid[quid]
        ua_score = ct_unit_d[0]['unit_annotation_score']
        score_dict = split_score(ua_score)
    else:
        score_dict = dict()
        score_dict['true'] = 0.0
        score_dict['false'] = 0.0
    return score_dict['true']



def aggregate_binary_labels(data_dict_list, ct_units):
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    units_by_quid = sort_by_key(ct_units, ['unit'])
    ct_thresholds = [0.5, 0.6, 0.7, 0.8, 0.9, 1]
    aggregated_binary_labels = []
    for pair, data_dicts in data_by_pair.items():
        if not pair.startswith('_'):
            data_by_rel = sort_by_key(data_dicts, ['relation'])
            prop_rels = defaultdict(list)
            ct_rels = defaultdict(dict)
            triple_dicts = []
            for rel, data in data_by_rel.items():
                answers = [d['answer'] for d in data]
                true_cnt = answers.count(True)
                prop = true_cnt/len(answers)
                prop_rels[prop].append(rel)
                majority_vote = False
                if prop > 0.5:
                    majority_vote = True
                triple_dict = dict()
                triple_dict['relation'] = rel.strip()
                triple_dict['workerid'] = 'aggregated'
                #triple_dict['level'] = rel_level_mapping[rel]
                triple_dict['property'] = pair.split('-')[0]
                triple_dict['concept'] = pair.split('-')[1]
                triple_dict['majority_vote'] = majority_vote
                triple_dict['completionurl'] = 'aggregated'
                # Get crowd truth scores
                triple = f'{rel}-{pair}'
                quid = data[0]['quid']
                for ct_thresh in ct_thresholds:
                    ct_score = get_ua_score(quid, units_by_quid)
                    if ct_score in ct_rels[ct_thresh].keys():
                        ct_rels[ct_thresh][ct_score].append(rel)
                    else:
                        ct_rels[ct_thresh][ct_score] = [rel]
                    if ct_score > ct_thresh:
                        ct_vote = True
                    else:
                        ct_vote = False
                    triple_dict[f'ct_vote_{ct_thresh}'] = ct_vote
                triple_dicts.append(triple_dict)
            # add top label
            top_prop = max(prop_rels.keys())
            for d in triple_dicts:
                rel = d['relation']
                if rel in prop_rels[top_prop]:
                    d['top_vote'] = True
                else:
                    d['top_vote'] = False

            for ct_thresh in ct_thresholds:
                top_ct = max(ct_rels[ct_thresh].keys())
                for d in triple_dicts:
                    rel = d['relation']
                    if rel in ct_rels[ct_thresh][top_ct]:
                        d[f'top_vote_ct_{ct_thresh}'] = True
                    else:
                        d[f'top_vote_ct_{ct_thresh}'] = False

            aggregated_binary_labels.extend(triple_dicts)
    return aggregated_binary_labels




def main():
    run = '4'
    batch = '100'
    n_q = '*'
    group = 'experiment2'

    # Total without filter
    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    print(len(data_dict_list))
    #data_filter = 'None'
    data_dict_list_clean = data_dict_list


    # collapse:
    #data_dict_list_coll = get_collapsed_relations(data_dict_list_clean,
                                                      # mapping = 'negative_relations')
    print(len(data_dict_list_clean))


    ct_units = load_ct('*', 'experiment*', '*', 'units', as_dict=True)
    aggregated_labels = aggregate_binary_labels(data_dict_list_clean)
    print(len(aggregated_labels))
    print(aggregated_labels[0])

    aggregated_labels_collapsed = get_collapsed_relations(aggregated_labels,
                                                          mapping='levels',
                                                         answer_name = 'ct_vote_0.6')

    print(aggregated_labels_collapsed[3])



if __name__ == '__main__':
    main()
