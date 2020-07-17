
from utils_data import load_experiment_data, load_expert_data, load_gold_data
from utils_data import load_config
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from clean_annotations import clean_workers
from aggregation import aggregate_binary_labels
from calculate_iaa import  get_collapsed_relations, get_agreement

from sklearn.metrics import precision_recall_fscore_support as p_r_f1
import pandas as pd
import argparse



def get_evaluation_instances(crowd, gold):
    triples_gold = sort_by_key(gold, ['relation', 'property', 'concept'])
    triples_crowd = sort_by_key(crowd, ['relation', 'property', 'concept'])
    evaluation_instances_crowd = []
    for t, gold_data in triples_gold.items():
        evaluation_instances_crowd.extend(triples_crowd[t])
        if len(triples_crowd[t]) == 0:
            print(t, 'no data')
    print(len(triples_gold), len(triples_crowd), len(evaluation_instances_crowd))
    return evaluation_instances_crowd


def evaluate(gold, crowd, vote):
    crowd_by_triple = sort_by_key(crowd, ['relation', 'property', 'concept'])
    gold_by_triple = sort_by_key(gold, ['relation', 'property', 'concept'])
    total = []
    labels_gold = []
    labels_crowd = []
    cov = 0
    for t, gold_data in gold_by_triple.items():
        crowd_data = crowd_by_triple[t]
        if len(crowd_data) == 1:
            cov +=1
            gold_answer = str(gold_data[0]['answer']).lower().strip()
            crowd_answer = str(crowd_data[0][vote]).lower().strip()
            gold_answer = str(gold_answer).lower()
            crowd_answer = str(crowd_answer).lower()
            labels_gold.append(gold_answer)
            labels_crowd.append(crowd_answer)
        else:
            print('irregularities in data:', t, len(crowd_data))
            #pass

    p, r, f1, support = p_r_f1(labels_gold, labels_crowd, average = 'weighted')
    results_dict = dict()
    results_dict['f1'] = f1
    results_dict['p'] = p
    results_dict['r'] = r
    results_dict['coverage'] = cov/len(gold)
    return results_dict


def evaluate_all_versions(gold, crowd_eval_agg, vote):
    versions = ['relations', 'levels'] #, 'negative_relations']
    results_dict = dict()

    for v in versions:
        if v != 'relations':
            gold_coll = get_collapsed_relations(gold,
                                    mapping=v, answer_name = 'answer')
            crowd_eval_agg_coll = get_collapsed_relations(crowd_eval_agg,
                                    mapping=v, answer_name = vote)
        else:
            crowd_eval_agg_coll = crowd_eval_agg
            gold_coll = gold
        res = evaluate(gold_coll, crowd_eval_agg_coll, vote=vote)
        for m, score in res.items():
            if v == 'levels':
                v = 'subset'
            results_dict[f'{v}-{m}'] = score
    return results_dict


def evaluate_configs(gold, crowd, ct_thresholds, stds):
    overview_dicts = []

    gold_labels = [str(d['answer']).lower() for d in gold]
    print('----Label distribution----')
    print('True:', gold_labels.count('true'))
    print('False', gold_labels.count('false'))
    print('----------------------------')

    ct_units = load_ct('*', 'experiment*', '*', 'units', as_dict=True)
    crowd_eval = get_evaluation_instances(crowd, gold)
    crowd_eval_agg = aggregate_binary_labels(crowd_eval, ct_units, ct_thresholds)
    iaa = get_agreement(crowd_eval, collapse_relations = False, v=False, disable_kappa=True)

    print('aggretation')
    print('no filtering - different aggretation methods')
    votes = ['majority_vote', 'top_vote', 'uas']
    for vote in votes:
        if vote in ['uas']:
            for thresh in ct_thresholds:
                vote_thresh = f'{vote}-{thresh}'
                results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote_thresh)
                results_dict['filtering'] = '-'
                results_dict['aggregation'] = f'uas-{thresh}'
                results_dict['alpha'] = iaa['Krippendorff']
                results_dict['filtering_unit'] = '-'
                results_dict['n_stdv'] = '-'
                results_dict['coverage'] = len(crowd_eval_agg)/len(gold)
                overview_dicts.append(results_dict)
        else:
            results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
            results_dict['filtering'] = '-'
            results_dict['aggregation'] = vote
            results_dict['filtering_unit'] ='-'
            results_dict['n_stdv'] = '-'
            results_dict['alpha'] = iaa['Krippendorff']
            results_dict['coverage'] = len(crowd_eval_agg)/len(gold)
            overview_dicts.append(results_dict)

    print('cleaning and aggregation')
    units = ['pair', 'batch', 'total']
    metrics = ['contradictions', 'ct_wqs']
    votes = ['majority_vote', 'top_vote']
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'

    for unit in units:
        for n_stdv in stds:
            for metric in metrics:
                crowd_eval_clean = clean_workers(crowd_eval, run, group,
                                               batch, metric, unit, n_stdv)

                iaa = get_agreement(crowd_eval_clean,
                collapse_relations = False, v=False,
                disable_kappa=True)
                crowd_eval_agg = aggregate_binary_labels(crowd_eval_clean,\
                                                        ct_units, ct_thresholds)
                if len(crowd_eval_agg) > 0:
                    for vote in votes:
                        results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
                        results_dict['filtering'] = metric
                        results_dict['aggregation'] = vote
                        results_dict['filtering_unit'] = unit
                        results_dict['n_stdv'] = n_stdv
                        results_dict['alpha'] = iaa['Krippendorff']
                        results_dict['coverage'] = len(crowd_eval_agg)/len(gold)
                        overview_dicts.append(results_dict)

                elif len(crowd_eval_clean) == 0:
                    print('not enough remaining data: ', unit, n_stdv, metric, vote)

    print('clean all contradictory annotations')
    unit = '-'
    n_stdv = '-'
    metric = 'exclude_contradictory_annotations'
    votes = ['majority_vote', 'top_vote']
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'
    crowd_eval_clean = clean_workers(crowd_eval, run, group, batch, metric, unit, n_stdv)
    iaa = get_agreement(crowd_eval_clean, collapse_relations = False, v=False, disable_kappa=True)
    crowd_eval_agg = aggregate_binary_labels(crowd_eval_clean, ct_units, ct_thresholds)
    for vote in votes:
        results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
        results_dict['filtering'] = metric
        results_dict['aggregation'] = vote
        results_dict['filtering_unit'] = unit
        results_dict['n_stdv'] = n_stdv
        results_dict['alpha'] = iaa['Krippendorff']
        results_dict['coverage'] = len(crowd_eval_agg)/len(gold)
        overview_dicts.append(results_dict)

    return overview_dicts


def main():

    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']

    parser = argparse.ArgumentParser()

    parser.add_argument("--ct_thresholds", \
                default=[0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 1],\
                type=list, nargs='+')
    parser.add_argument("--stds", \
                default=[ 0.5, 1, 1.5, 2],\
                type=list, nargs='+')

    # aggregation parameters:
    args = parser.parse_args()
    ct_thresholds = args.ct_thresholds
    stds = args.stds

    # load crowd:
    crowd = load_experiment_data(run, group, n_q, batch)

    # load full gold set
    gold = load_gold_data()
    gold = [d for d in gold if d['answer'] != 'NOGOLD']

    # evaluate agree category:
    gold_by_agreement = sort_by_key(gold, ['expected_agreement'])
    gold_agree = gold_by_agreement['agreement']
    gold_poss_disagree = gold_by_agreement['possible_disagreement']
    gold_disagree = gold_by_agreement['disagreement']
    # merge possible with certain disagreement
    gold_disagree_all = []
    gold_disagree_all.extend(gold_poss_disagree)
    gold_disagree_all.extend(gold_disagree)

    # evaluate total
    overview_dicts = evaluate_configs(gold, crowd, ct_thresholds, stds)
    df =  pd.DataFrame(overview_dicts)
    df_total = df.sort_values(by=['relations-f1'], ascending=False)[['filtering',
                                                           'filtering_unit',
                                                           'n_stdv',
                                                           'aggregation',
                                                            'relations-f1',
                                                            'relations-p',
                                                           'relations-r',
                                                           'alpha', 'relations-coverage']]
    df_total.round(2).to_csv('../evaluation/evaluation_accuracy_full.csv')

    # evaluate disagreement and agreement
    overview_dicts_agree = evaluate_configs(gold_agree, crowd, ct_thresholds, stds)
    overview_dicts_disagree = evaluate_configs(gold_disagree_all, crowd,ct_thresholds, stds)
    for d in overview_dicts_agree:
        d['behav.'] = 'agree'
    for d in overview_dicts_disagree:
        d['behav.'] = 'disagree'
    overview_dicts_total = []
    overview_dicts_total.extend(overview_dicts_agree)
    overview_dicts_total.extend(overview_dicts_disagree)

    df =  pd.DataFrame(overview_dicts_total)
    df = df.sort_values(by=['relations-f1'], ascending=False)[['behav.', 'filtering',
                                                               'filtering_unit',
                                                               'n_stdv',
                                                               'aggregation',
                                                                'relations-f1',
                                                                'relations-p',
                                                               'relations-r',
                                                               'alpha']]
    df.round(2).to_csv('../evaluation/evaluation_accuracy_agree_disagree.csv')
    df

if __name__ == '__main__':
    main()
