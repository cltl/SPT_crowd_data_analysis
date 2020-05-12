# did relation evaluation
# ADD example evaluation


from load_data import load_experiment_data
from calculate_iaa import get_agreement
from utils_analysis import load_contradiction_pairs
from utils_analysis import collect_contradictions
from utils_analysis import sort_by_key
from utils_analysis import get_annotation_ids

from collections import Counter, defaultdict
import pandas as pd
import os

def get_agreement_by_relation(data_dict_list):

    agreement_rel_dict = dict()
    data_by_relation = sort_by_key(data_dict_list, ['relation'])
    for rel, dl_rel in data_by_relation.items():
        agreement_rel_dict[rel] = get_agreement(dl_rel, v=False)
    return agreement_rel_dict

def get_agreement_by_example(data_dict_list):

    agreement_ex_dict = dict()
    relation_examples_ag_dict = dict()
    data_by_relation = sort_by_key(data_dict_list, ['relation'])
    for rel, dl_rel in data_by_relation.items():
        data_by_ex = sort_by_key(dl_rel, ['exampletrue', 'examplefalse'])
        agreement_ex_dict = dict()
        for ex, dl_ex in data_by_ex.items():
            agreement_ex_dict[ex] = get_agreement(dl_ex, v=False)
            agreement_ex_dict[ex]['n_annotations'] = n_annotations = len(dl_ex)
        relation_examples_ag_dict[rel]  = agreement_ex_dict
    return relation_examples_ag_dict


def agreement_relations_across_runs(runs, experiment_name):

    run_rel_dict = dict()
    batch = '*'
    n_q = '*'

    run_rel_dict = dict()
    for run in runs:
        data_dict_list = load_experiment_data(run, experiment_name, n_q, batch,\
                                              remove_not_val = True)
        name = f'run{run}-group_{experiment_name}-batch{batch}'.replace('*', '-all-')
        agreement_rel_dict = get_agreement_by_relation(data_dict_list)
        run_rel_dict[run] = agreement_rel_dict

    relations = set()
    for run, rel_dict in run_rel_dict.items():
        relations.update(rel_dict.keys())

    line_dicts = []
    for rel in relations:
        line_dict = dict()
        line_dict['relation'] = rel
        for run, rel_dict in run_rel_dict.items():
            if rel in rel_dict:
                ag_dict = rel_dict[rel]
                for m, ag in ag_dict.items():
                    line_dict[f'{run}_{m}'] = ag
        line_dicts.append(line_dict)
    df = pd.DataFrame(line_dicts)
    dir_name = '../analyses/iaa/'
    f_name = f'relations_runs{"-".join(runs)}.csv'
    path = f'{dir_name}{f_name}'
    df = pd.DataFrame(line_dicts)
    df.to_csv(path)
    return path, df

def agreement_examples_across_runs(runs, experiment_name):

    run_rel_dict = dict()
    batch = '*'
    n_q = '*'

    run_rel_dict = dict()
    for run in runs:
        data_dict_list = load_experiment_data(run, experiment_name, n_q, batch,\
                                              remove_not_val = True)
        name = f'run{run}-group_{experiment_name}-batch{batch}'.replace('*', '-all-')
        relation_examples_ag_dict = get_agreement_by_example(data_dict_list)
        run_rel_dict[run] = relation_examples_ag_dict

    relations = set()
    for run, rel_dict in run_rel_dict.items():
        relations.update(rel_dict.keys())

    line_dicts = []
    for rel in relations:
        for run, rel_dict in run_rel_dict.items():
            if rel in rel_dict:
                example_ag_dict = rel_dict[rel]
                for ex, ag_dict in example_ag_dict.items():
                    line_dict = dict()
                    line_dict['relation'] = rel
                    line_dict['example'] = ex
                    for m, ag in ag_dict.items():
                        line_dict[f'{run}_{m}'] = ag
                    line_dicts.append(line_dict)
    df = pd.DataFrame(line_dicts)
    dir_name = '../analyses/iaa/'
    f_name = f'examples_runs{"-".join(runs)}.csv'
    path = f'{dir_name}{f_name}'
    df = pd.DataFrame(line_dicts)
    df.to_csv(path)
    return path, df


def main():
    # analyze all data:
    runs = ['3', '1']
    experiment_name = 'experiment1'
    path, df = agreement_relations_across_runs(runs, experiment_name)
    print(f'Results written to: {path}')
    path, df = agreement_examples_across_runs(runs, experiment_name)
    print(f'Results written to: {path}')

if __name__ == '__main__':
    main()
