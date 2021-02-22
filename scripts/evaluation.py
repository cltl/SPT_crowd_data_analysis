import csv
from utils_data import load_gold_data, load_processed_data
from utils_analysis import sort_by_key
from sklearn.metrics import precision_recall_fscore_support as p_r_f1
from calculate_iaa import  get_collapsed_relations, get_agreement
import pandas as pd

def load_aggregated_data(runs, vote, source):
    path_dir = f'../data/aggregated/'
    path_f = f'run{"_".join(runs)}-group_all--batch-all--{source}-{vote}.csv'
    # run3_4-group_all--batch-all--clean_contradictions_batch_0.5-uas1.0.csv
    print(f'{path_dir}{path_f}')
    with open(f'{path_dir}{path_f}') as infile:
        data = list(csv.DictReader(infile))
    return data


def get_evaluation_instances(crowd, gold, verbose=False):
    triples_gold = sort_by_key(gold, ['relation', 'property', 'concept'], key_type='tuple')
    triples_crowd = sort_by_key(crowd, ['relation', 'property', 'concept'], key_type='tuple')
    evaluation_instances_crowd = []
    for t, gold_data in triples_gold.items():
        evaluation_instances_crowd.extend(triples_crowd[t])
        if len(triples_crowd[t]) == 0 and verbose == True:
            print(t, 'no data')
    print(len(triples_gold), len(triples_crowd), len(evaluation_instances_crowd))
    return evaluation_instances_crowd


def evaluate(gold, crowd, verbose=False):
    crowd_by_triple = sort_by_key(crowd, ['relation', 'property', 'concept'], key_type='tuple')
    gold_by_triple = sort_by_key(gold, ['relation', 'property', 'concept'], key_type='tuple')
    total = []
    labels_gold = []
    labels_crowd = []
    cov = 0
    for t, gold_data in gold_by_triple.items():
        crowd_data = crowd_by_triple[t]
        if len(crowd_data) == 1:
            cov += 1
            gold_answer = str(gold_data[0]['answer']).lower().strip()
            crowd_answer = str(crowd_data[0]['label']).lower().strip()
            labels_gold.append(gold_answer)
            labels_crowd.append(crowd_answer)
            # print(t, crowd_answer, gold_answer, crowd_answer == gold_answer)
        elif verbose == True:
            print('irregularities in data:', t, len(crowd_data))
            # pass

    p, r, f1, support = p_r_f1(labels_gold, labels_crowd, average='weighted')
    results_dict = dict()
    results_dict['f1'] = f1
    results_dict['p'] = p
    results_dict['r'] = r
    results_dict['coverage'] = cov / len(gold)
    return results_dict


def evaluate_configs(gold):
    runs = ['3', '4']
    votes = ['majority', 'top',
             'uas1.0', 'uas0.5', 'uas0.55', 'uas0.6',
             'uas0.65', 'uas0.7', 'uas0.75', 'uas0.8',
             'uas0.85', 'uas0.9', 'uas0.95', 'uas1.0']

    units = ['batch', 'pair', 'total']
    metrics = ['contradictions', 'time-below']
    n_stds = [0.5, 1, 1.5, 2]

    sources = ['data_processed']
    for unit in units:
        for n_st in n_stds:
            for metric in metrics:
                if metric == 'time-below':
                    unit = 'batch'
                source = f'clean_{metric}_{unit}_{n_st}'
                if source not in sources:
                    sources.append(source)
            # 'clean_contradictions_batch_1', 'clean_contradictions_pairs_0.5']

    results_dicts = []
    for source in sources:

        group = '*'
        n_q = '*'
        n_lists = '*'
        batch = '*'

        data = []
        for run in runs:
            data.extend(load_processed_data(run, group, n_q, n_lists,
                                            batch, source, verbose=False))
        print('total annotated data:', len(data), source)
        data_eval = get_evaluation_instances(data, gold)

        for vote in votes:
            if vote.startswith('uas') and source in ['data_processed',
                                                     'clean_contradictions_batch_0.5',
                                                     'clean_contradictions_pair_0.5']:  # , 'clean_contradictions_batch_0.5']:

                crowd = load_aggregated_data(runs, vote, source)
                print(source, vote, len(crowd))
                if len(data_eval) != 0:
                    iaa = get_agreement(data_eval, collapse_relations=False, v=False, disable_kappa=True)
                    alpha = iaa['Krippendorff']
                else:
                    alpha = None
                results_dict = dict()
                results_dict['filter'] = source
                results_dict['vote'] = vote
                results_dict.update(evaluate(gold, crowd))
                results_dict['alpha'] = alpha
                results_dicts.append(results_dict)
            elif not vote.startswith('uas'):
                crowd = load_aggregated_data(runs, vote, source)
                print(source, vote, len(crowd))
                if len(data_eval) != 0:
                    iaa = get_agreement(data_eval, collapse_relations=False, v=False, disable_kappa=True)
                    alpha = iaa['Krippendorff']
                else:
                    alpha = None
                results_dict = dict()
                results_dict['filter'] = source
                results_dict['vote'] = vote
                results_dict.update(evaluate(gold, crowd))
                results_dict['alpha'] = alpha
                results_dicts.append(results_dict)
    return results_dicts



def main():

    # evaluate total:
    gold = load_gold_data()
    print(gold[0].keys())
    print(len(gold))
    # remove no gold:
    gold = [d for d in gold if d['answer'] != 'NOGOLD']
    results_dicts = evaluate_configs(gold)
    df = pd.DataFrame(results_dicts).sort_values('f1', ascending=False)
    df.to_csv('../evaluation/evaluation_accuracy_full_update.csv')

    # evaluate expectation sets:
    # evaluate agree category:
    gold = load_gold_data()
    # evaluate agree category:
    gold_by_agreement = sort_by_key(gold, ['expected_agreement'])
    gold_agree = gold_by_agreement['agreement']
    gold_poss_disagree = gold_by_agreement['possible_disagreement']
    gold_disagree = gold_by_agreement['disagreement']
    print('gold agree', len(gold_agree))
    print('gold poss disagree', len(gold_poss_disagree))
    print('gold disagree', len(gold_disagree))
    # merge possible with certain disagreement
    gold_disagree_all = []
    gold_disagree_all.extend(gold_poss_disagree)
    gold_disagree_all.extend(gold_disagree)

    results_dicts_agree = evaluate_configs(gold_agree)
    results_dicts_disagree = evaluate_configs(gold_disagree_all)

    for d in results_dicts_agree:
        d['behav.'] = 'agree'
    for d in results_dicts_disagree:
        d['behav.'] = 'disagree'
    overview_dicts_total = []
    overview_dicts_total.extend(results_dicts_agree)
    overview_dicts_total.extend(results_dicts_disagree)

    df = pd.DataFrame(overview_dicts_total).sort_values('f1', ascending=False)
    df.to_csv('../evaluation/evaluation_accuracy_agree_disagree_update.csv')

if __name__ == '__main__':
    main()