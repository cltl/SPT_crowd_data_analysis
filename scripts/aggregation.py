# aggretation dev
from utils_data import load_experiment_data, load_config
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from calculate_iaa import get_collapsed_relations
from clean_annotations import remove_contradicting_workers, clean_workers


from collections import defaultdict, Counter
import pandas as pd
import os
import argparse


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
        print('quid not found:', quid)
        score_dict = dict()
        score_dict['true'] = 0.0
        score_dict['false'] = 0.0
    return score_dict['true']


def get_propertion_true(data):
    answers = [d['answer'] for d in data]
    true_cnt = answers.count('true')
    prop = true_cnt/len(answers)
    return prop



def get_agg_dict(data, pair, rel):
    triple_dict = dict()
    triple_dict['relation'] = rel.strip()
    triple_dict['workerid'] = 'aggregated'
    triple_dict['quid'] = data[0]['quid']
    triple_dict['property'] = pair.split('-')[0]
    triple_dict['concept'] = pair.split('-')[1]
    triple_dict['completionurl'] = 'aggregated'
    return triple_dict


def get_props(data_by_rel):
    rel_prop_dict = dict()
    for rel, data in data_by_rel.items():
        prop = get_propertion_true(data)
        rel_prop_dict[rel] = prop
    return rel_prop_dict

def get_cts(data_by_rel, units_by_quid):
    rel_ct_score_dict= dict()
    for rel, data in data_by_rel.items():
        quid = data[0]['quid']
        quids = set([d['quid'] for d in data])
        #if len(quids) >1:
        #    for d in data:
        #        print(d['description'])
        #else:
        #    print('expected length of 1')
        ct_score = get_ua_score(quid, units_by_quid)
        rel_ct_score_dict[rel] = ct_score
    return rel_ct_score_dict


def get_majority_vote(rel_prop_dict):
    rel_vote_dict = dict()
    for rel, prop in rel_prop_dict.items():
        if prop > 0.5:
            rel_vote_dict[rel] = True
        else:
            rel_vote_dict[rel] = False
    return rel_vote_dict

def get_top_vote(rel_prop_dict):
    rel_vote_dict = dict()

    prop_rels = defaultdict(list)
    for rel, prop in rel_prop_dict.items():
        prop_rels[prop].append(rel)
    top_prop = max(prop_rels.keys())
    top_rels = prop_rels[top_prop]
    for rel in rel_prop_dict.keys():
        if rel in top_rels:
            vote = True
        else:
            vote = False
        rel_vote_dict[rel] = vote
    return rel_vote_dict


def get_ct_vote(rel_ct_score_dict, thresh):
    rel_vote_dict = dict()
    for rel, ct_score in rel_ct_score_dict.items():
        if ct_score >= thresh:
            rel_vote_dict[rel] = True
        else:
            rel_vote_dict[rel] = False
    return rel_vote_dict

def aggregate_binary_labels(data_dict_list, ct_units, ct_thresholds):
    aggregated_binary_labels = []
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    units_by_quid = sort_by_key(ct_units, ['unit'])
    for pair, data_dicts_pair in data_by_pair.items():
        if not pair.startswith('_'):
            data_by_rel = sort_by_key(data_dicts_pair, ['relation'])
            # collect scores/propertions:
            rel_prop_dict = get_props(data_by_rel)
            rel_ct_score_dict = get_cts(data_by_rel, units_by_quid)

            rel_majority_vote = get_majority_vote(rel_prop_dict)
            rel_top_vote = get_top_vote(rel_prop_dict)
            ct_thresh_votes = dict()
            for thresh in ct_thresholds:
                ct_thresh_votes[thresh] = get_ct_vote(rel_ct_score_dict, thresh)
            for rel, data in data_by_rel.items():
                triple_dict = get_agg_dict(data, pair, rel)
                triple_dict['majority_vote'] = rel_majority_vote[rel]
                triple_dict['top_vote'] = rel_top_vote[rel]
                if len(ct_thresh_votes) > 0:
                    for thresh, ct_vote in ct_thresh_votes.items():
                        triple_dict[f'uas-{thresh}'] = ct_vote[rel]
                aggregated_binary_labels.append(triple_dict)
    return aggregated_binary_labels

def labels_to_csv(path, aggregated_labels, vote):
    aggregated_df = pd.DataFrame(aggregated_labels)
    cols = ['relation', 'property', 'concept', vote]
    df = aggregated_df[cols]
    df.to_csv(path)


def main():

    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']
    n_lists = '*'
    parser = argparse.ArgumentParser()
    parser.add_argument("--votes", default=['majority_vote'], type=list, nargs="+")

    parser.add_argument("--ct_thresholds", default= [],\
                                             type=list, nargs="+")

    parser.add_argument("--metric_clean", default='contradictions',type=str )
    parser.add_argument("--unit_clean", default='batch',type=str )
    parser.add_argument("--n_stdv_clean", default=0.5 ,type=float )

    # ct_thresholds = default= [0.5, 0.55, 0.6, 0.65, 0.7,\
                                    #    0.75, 0.8, 0.85, 0.9, 1]
    # aggregation parameters:
    args = parser.parse_args()
    votes = args.votes
    ct_thresholds = args.ct_thresholds

    # filtering parameter:
    metric = args.metric_clean
    unit = args.unit_clean
    n_stdv = args.n_stdv_clean

    # Total without filter
    data_dict_list = load_experiment_data(run, group, n_q, n_lists, batch, remove_not_val = True)
    print(len(data_dict_list))


    if metric != 'raw':
        data_dict_list_clean = clean_workers(data_dict_list, run, group, batch, metric, unit, n_stdv)
    else:
        data_dict_list_clean = data_dict_list
    print(len(data_dict_list_clean))

    # aggregate:
    ct_units = load_ct(run, group, batch, 'units', as_dict=True)
    aggregated_labels = aggregate_binary_labels(data_dict_list_clean, ct_units, ct_thresholds)

    # to csv
    for vote in votes:
        name = f'run{run}-group_{group}-batch{batch}-cleaned_{metric}_{unit}_{n_stdv}-vote_{vote}-relations'
        name = name.replace('*', '-all-')
        path = f'../aggregated_labels/{name}.csv'
        labels_to_csv(path, aggregated_labels, vote)
        print('Result written to:', path)

        aggregated_labels_collapsed = get_collapsed_relations(aggregated_labels,
                                                              mapping='levels',
                                                             answer_name = vote)
        name = f'run{run}-group_{group}-batch{batch}-cleaned_{metric}_{unit}_{n_stdv}-vote_{vote}-levels'
        name = name.replace('*', '-all-')
        path = f'../aggregated_labels/{name}.csv'
        print('Result written to:', path)
        labels_to_csv(path, aggregated_labels_collapsed, vote)


if __name__ == '__main__':
    main()
