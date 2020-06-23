from clean_annotations import clean_workers
from load_data import load_experiment_data, load_expert_data, load_gold_data
from aggregation import aggregate_binary_labels
from calculate_iaa import  get_collapsed_relations
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct

from sklearn.metrics import precision_recall_fscore_support as p_r_f1
import pandas as pd



def get_evaluation_instances(crowd, gold):
    triples_gold = sort_by_key(gold, ['relation', 'property', 'concept'])
    triples_crowd = sort_by_key(crowd, ['relation', 'property', 'concept'])

    evaluation_instances_crowd = []
    for t, gold_data in triples_gold.items():
        evaluation_instances_crowd.extend(triples_crowd[t])
    print(len(triples_gold), len(triples_crowd), len(evaluation_instances_crowd))
    return evaluation_instances_crowd


def evaluate(gold, crowd, vote):
    crowd_by_triple = sort_by_key(crowd, ['relation', 'property', 'concept'])
    gold_by_triple = sort_by_key(gold, ['relation', 'property', 'concept'])
    total = []
    labels_gold = []
    labels_crowd = []
    for t, gold_data in gold_by_triple.items():
        crowd_data = crowd_by_triple[t]
        if len(crowd_data) == 1:
            gold_answer = str(gold_data[0]['answer']).lower().strip()
            crowd_answer = str(crowd_data[0][vote]).lower().strip()
            #print(t, gold_answer, crowd_answer)
            #if gold_answer != crowd_answer:
            #    print(gold_answer, crowd_answer, t, vote)
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
    return results_dict


def evaluate_all_versions(gold, crowd_eval_agg, vote):
    versions = ['relations', 'levels', 'negative_relations']
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
            results_dict[f'{v}-{m}'] = score
    return results_dict


def evaluate_configs(gold, crowd):
    overview_dicts = []

    ct_units = load_ct('*', 'experiment*', '*', 'units', as_dict=True)
    crowd_eval = get_evaluation_instances(crowd, gold)
    crowd_eval_agg = aggregate_binary_labels(crowd_eval, ct_units)

    print('aggretation')

    votes = ['majority_vote', 'top_vote', 'ct_vote_0.5', 'ct_vote_0.6',
                 'ct_vote_0.7', 'ct_vote_0.8', 'ct_vote_0.9',
                 'top_vote_ct_0.5', 'top_vote_ct_0.6','top_vote_ct_0.7',
                 'top_vote_ct_0.8', 'top_vote_ct_0.9']
    for vote in votes:
        results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
        config = (vote)
        results_dict['config'] = config
        overview_dicts.append(results_dict)

    print('cleaning and aggregation')
    units = ['pair', 'batch', 'total']
    stds = [ 0.5, 1, 1.5, 2]
    metrics = ['contradictions', 'crowdtruth']
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
                crowd_eval_agg = aggregate_binary_labels(crowd_eval_clean, ct_units)
                for vote in votes:
                    results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
                    config = (unit, n_stdv, metric, vote)
                    results_dict['config'] = config
                    overview_dicts.append(results_dict)

    print('clean all contradictory annotations')
    unit = None
    n_stdv = None
    metric = 'exclude_contradictory_annotations'
    votes = ['majority_vote', 'top_vote']
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'
    for vote in votes:
        crowd_eval_clean = clean_workers(crowd_eval, run, group, batch, metric, unit, n_stdv)
        results_dict = evaluate_all_versions(gold, crowd_eval_agg, vote)
        config = (unit, n_stdv, metric, vote)
        results_dict['config'] = config
        overview_dicts.append(results_dict)

    return overview_dicts


def main():


    run = 4
    group = 'expert_inspection1'
    n_q = '*'
    batch = '*'
    gold = load_gold_data(run, group, n_q, batch)

    # load crowd:
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'
    crowd = load_experiment_data(run, group, n_q, batch)
    overview_dicts = evaluate_configs(gold, crowd)
    df =  pd.DataFrame(overview_dicts)
    print(df.sort_values(by=['relations-f1'], ascending=False)[['config',
                                                          'relations-f1',
                                                          'levels-f1', 'negative_relations-f1']])
if __name__ == '__main__':
    main()
