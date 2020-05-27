
from sklearn.metrics import precision_recall_fscore_support as p_r_f1


from load_data import load_expert_data
from load_data import load_experiment_data
from utils_analysis import load_analysis
from utils_analysis import sort_by_key
from aggregation import aggregate_binary_labels
from clean_annotations import remove_contradicting_workers



def evaluate(expert_bin_labels, crowd_bin_labels, vote):
    crowd_by_triple = sort_by_key(crowd_bin_labels, ['relation', 'pair'])
    expert_by_triple = sort_by_key(expert_bin_labels, ['relation', 'pair'])

    total = []
    labels_exp = []
    labels_crowd = []
    for t, exp_data in expert_by_triple.items():
        crowd_data = crowd_by_triple[t]
        exp_answer = exp_data[0][vote]
        crowd_answer = crowd_data[0][vote]
        labels_exp.append(exp_answer)
        labels_crowd.append(crowd_answer)
    #acc = n_corr/len(total)
    p, r, f1, support = p_r_f1(labels_exp, labels_crowd, average = 'weighted')
    results_dict = dict()
    results_dict['f1'] = f1
    results_dict['p'] = p
    results_dict['r'] = r
    return results_dict


def main():

    # load expert data
    run = 4
    group = 'expert_inspection*'
    n_q = '*'
    batch = '*'
    data_dict_list = load_expert_data(run, group, n_q, batch)

    # load crowd data:
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'
    data_dict_list_crowd = load_experiment_data(run, group, n_q, batch)
    # clean in preferred way:
    unit = 'pair'
    n_stds = 0.5
    if unit == 'total':
        analysis_type = 'workers'
    else:
        analysis_type = f'workers_by_{unit}'
    dict_list_workers = load_analysis(analysis_type, run, group, batch, as_dict = True)
    data_dict_list_crowd_clean= remove_contradicting_workers(data_dict_list_crowd, dict_list_workers, unit,  n_stds)

    # aggregate binary labels
    expert_bin_labels = aggregate_binary_labels(data_dict_list)
    crowd_bin_labels_raw  = aggregate_binary_labels(data_dict_list_crowd)
    crowd_bin_labels_clean  = aggregate_binary_labels(data_dict_list_crowd_clean)


    # with clean data
    results_dict = evaluate(expert_bin_labels, crowd_bin_labels_clean, vote='majority_vote')
    print(results_dict)

    #with raw data
    results_dict = evaluate(expert_bin_labels, crowd_bin_labels_raw, vote='majority_vote')
    print(results_dict)

if __name__ == '__main__':
    main()
