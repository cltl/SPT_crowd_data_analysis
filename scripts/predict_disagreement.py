from clean_annotations import clean_workers
from aggregation import aggregate_binary_labels
from calculate_iaa import get_agreement

from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from utils_analysis import load_contradiction_pairs
from utils_analysis import collect_contradictions
from utils_data import load_experiment_data, load_gold_data, load_config

from sklearn.metrics import precision_recall_fscore_support as p_r_f1
from collections import defaultdict
from statistics import stdev
import argparse


def iaa_dis_agreement(data_dict_list, expert_unit_agreement_dict):

    data_by_agreement = defaultdict(list)
    data_by_triple = sort_by_key(data_dict_list, ['relation', 'property', 'concept'])

    for t, gold_expect in expert_unit_agreement_dict.items():
        data = data_by_triple[t]
        data_by_agreement[gold_expect].extend(data)


    for exp, data in data_by_agreement.items():
        agreement = get_agreement(data, v=False)
        data_by_triple = sort_by_key(data, ['relation', 'property', 'concept'])
        print(exp, agreement['Krippendorff'], len(data_by_triple))


def get_expected_behavior(gold):
    unit_behavior_dict = dict()
    for d in gold:
        unit =  f"{d['relation']}-{d['property']}-{d['concept']}"
        exp = d['expected_agreement']
        cnt = d['disagreement_cnt']
        if exp == 'disagreement' and cnt >= 3:
            exp = 'certain_disagreement'
        #if exp != 'agreement':
        unit_behavior_dict[unit] = exp
        #else:
            #unit_behavior_dict[unit] = 'agreement'
    return unit_behavior_dict



def get_agreement_by_unit(data_dict_list):

    agreement_unit_dict = dict()
    data_by_unit = sort_by_key(data_dict_list, ['relation', 'property', 'concept'])
    for unit, dl_unit in data_by_unit.items():
        agreement = get_agreement(dl_unit, v=False, disable_kappa=True)
        agreement_unit_dict[unit] = agreement['Proportional']
    return agreement_unit_dict


def get_agreement_by_pair(data_dict_list, ag_metric):

    agreement_unit_dict = dict()
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    for pair, dl_unit in data_by_pair.items():
        agreement = get_agreement(dl_unit, v=False, disable_kappa=True)
        for d in dl_unit:
            triple = f"{d['relation']}-{d['property']}-{d['concept']}"
            agreement_unit_dict[triple] = agreement[ag_metric]

    return agreement_unit_dict

def get_contradictions_by_pair(data_dict_list, pair_analysis):
    contradictions = load_contradiction_pairs()
    contradictions_unit_dict = dict()
    data_by_pair = sort_by_key(data_dict_list, ['property', 'concept'])
    analysis_by_pair = sort_by_key(pair_analysis, ['pair'])
    for pair, data_pair in data_by_pair.items():
        data_by_worker = sort_by_key(data_pair, ['workerid'])
        n_possible_contradictions = 0
        n_contradictions = 0
        for w, data in data_by_worker.items():
            pair_worker_cont = collect_contradictions(data, contradictions, threshold = 0)
            relations = [d['relation'] for d in data]
            for r1, r2 in contradictions:
                if r1 in relations and r2 in relations:
                    n_possible_contradictions += 1
            n_contradictions += len(pair_worker_cont)
        relations = set([d['relation'] for d in data_pair])
        for r in relations:
            unit = f'{r}-{pair}'
            if n_possible_contradictions == 0:
                contradictions_unit_dict[unit] = 0
            else:
                contradictions_unit_dict[unit] = n_contradictions/n_possible_contradictions

    return contradictions_unit_dict




def get_uqs_by_unit(data_dict_list, ct_units):
    ct_by_unit = sort_by_key(ct_units, ['unit'])
    uqs_unit_dict = dict()
    for d in data_dict_list:
        quid = d['quid']
        if quid in ct_by_unit:
            uqs = ct_by_unit[quid][0]['uqs']
            triple = f"{d['relation']}-{d['property']}-{d['concept']}"
            uqs_unit_dict[triple] = uqs
    return uqs_unit_dict


def disagreement_acc(target_units_true, target_units_false, unit_score_dict, thresh, below = True):

    predictions = []
    labels = [True for u in target_units_true]
    [labels.append(False) for u in target_units_false]
    target_units = target_units_true + target_units_false

    for u in target_units:
        score = unit_score_dict[u]
        if below == True:
            if score < thresh:
                predictions.append(True)
            else:
                predictions.append(False)
        elif below == False:
            if score > thresh:
                predictions.append(True)
            else:
                 predictions.append(False)
    p, r, f1, support = p_r_f1(labels, predictions, average = 'micro')
    correct_pos = []
    correct_neg = []
    for u, l, pred in zip(target_units, labels, predictions):
        if l == pred == True:
            correct_pos.append(u)
        elif l==pred==False:
            correct_neg.append(u)
    acc_true = len(correct_pos)/len(target_units_true)
    print('f1', round(f1, 2), round(acc_true, 2))
    return f1, acc_true

def get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = True):
    mean = sum(unit_score_dict.values())/len(unit_score_dict)
    sd = stdev(unit_score_dict.values())
    target_units_dis = [u for u, ex in expert_unit_agreement_dict.items() if ex == 'certain_disagreement']
    target_units_ag = [u for u, ex in expert_unit_agreement_dict.items() if ex == 'agreement']
    #print(len(target_units_dis), len(target_units_ag))
    f1_sd = []
    acc_true_sd = []
    for n_sd in n_stds:
        print('n_sd', n_sd)
        if invert == True:
            thresh = mean - (sd * n_sd)
            f1, acc_true = disagreement_acc(target_units_dis, target_units_ag,\
                                        unit_score_dict, thresh, below = True)
        elif invert == False:
            thresh = mean + (sd * n_sd)
            f1, acc_true = disagreement_acc(target_units_dis,\
                            target_units_ag, unit_score_dict, thresh, below = False)
        f1_sd.append((f1, n_sd))
        acc_true_sd.append((acc_true, n_sd))
    print(max(f1_sd), max(acc_true_sd))



def main():
    config_dict = load_config()
    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']
    parser = argparse.ArgumentParser()
    parser.add_argument("--unit_clean", default= 'batch', type=str)
    parser.add_argument("--std_clean", default=0.5, type=float)
    parser.add_argument("--metric_clean", default = 'contradictions', type = str)
    parser.add_argument("--n_stds_disagreement", \
                        default = [0, 0.5, 1, 1.5, 2],\
                        type = list, nargs = "+")

    # aggregation parameters:
    args = parser.parse_args()

    # load gold
    gold = load_gold_data()
    print('number of gold instances: ', len(gold))
    print()
    expert_unit_agreement_dict = get_expected_behavior(gold)

    # load crowd
    crowd = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    # clean crowd
    metric = args.metric_clean
    unit = args.unit_clean
    n_stdv_clean = args.std_clean
    n_stds = args.n_stds_disagreement
    crowd_clean = clean_workers(crowd, run, group, batch, metric, unit, n_stdv_clean)

    # load analyses
    analysis_type = 'units'
    ct_units = load_ct(run, group, batch, analysis_type, as_dict=True)
    analysis_type = 'pairs'
    pair_analysis =  load_analysis(analysis_type, run, group, batch, as_dict=True)


    # Agreement overview
    iaa_dis_agreement(crowd, expert_unit_agreement_dict)
    print()
    iaa_dis_agreement(crowd_clean, expert_unit_agreement_dict)
    print()

    # predict based on uqs
    unit_score_dict = get_uqs_by_unit(crowd, ct_units)
    get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = True)
    print()

    # predict based on proportional agreement on  full data
    unit_score_dict = get_agreement_by_unit(crowd)
    get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = True)
    print()

    # predict based on proportional agreement on clean data
    unit_score_dict = get_agreement_by_unit(crowd_clean)
    get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = True)
    print()

    # predict based on contradictions
    unit_score_dict = get_contradictions_by_pair(crowd, pair_analysis)
    get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = False)
    print()

    # predict based on contradictions clean
    unit_score_dict = get_contradictions_by_pair(crowd_clean, pair_analysis)
    get_score(expert_unit_agreement_dict, unit_score_dict, n_stds, invert = False)
    print()





if __name__ == '__main__':
    main()
