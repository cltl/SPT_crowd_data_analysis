#### NOT finished

from utils_analysis import load_analysis
from utils_analysis import sort_by_key
from utils_analysis import collect_contradictions
from utils_analysis import load_contradiction_pairs
from load_data import load_experiment_data
import numpy as np
from collections import Counter
import pandas as pd

def str_to_tuple(tuple_string):
    t = tuple(tuple_string.replace('(', '').replace(')', '').replace("'", "").replace(' ', '').split(','))
    return t


def get_pair_workers_df(pair_dicts, worker_dicts, data_dicts):
    pair_worker_dicts = []
    contradictions = load_contradiction_pairs()
    data_dicts_by_pairs = sort_by_key(data_dicts, ['property', 'concept'])
    pair_dicts_by_pairs = sort_by_key(pair_dicts, ['pair'])
    worker_dicts_by_worker = sort_by_key(worker_dicts, ['workerid'])
    for pair, dl in data_dicts_by_pairs.items():
        dl_by_workers = sort_by_key(dl, ['workerid'])
        for worker, dl_worker_pair in dl_by_workers.items():
            cont_worker_pair =  collect_contradictions(dl_worker_pair,\
                                                       contradictions, threshold = 0)
            cont_cnt = Counter()
            cont_cnt.update(cont_worker_pair)
            pair_worker_dict = dict()
            pair_worker_dict['pair'] = pair
            pair_worker_dict['workerid'] = worker
            pair_worker_dict.update(cont_cnt)
            worker_dict = worker_dicts_by_worker[worker][0]
            worker_tendencies = worker_dict['outlier_contradictions']
            if str(worker_tendencies) != 'nan':
                w_t_list = [str_to_tuple(s) for s in worker_tendencies.split(') (')]
                pair_worker_dict['worker_outliers']  = w_t_list
            else:
                w_t_list = []

            pair_conts = set(cont_worker_pair)
            worker_tends = set(w_t_list)

            overlap = pair_conts.intersection(worker_tends)
            if len(overlap) > 0:
                remove = True
            else:
                remove = False
            pair_worker_dict['tendency in contradictions'] = remove
            pair_worker_dicts.append(pair_worker_dict)
    df = pd.DataFrame.from_records(pair_worker_dicts)
    return df
            # check how many of the contradictions are also general tendencies of the worker
            #overlap_cont_tend

run = '*'
exp_name = 'experiment1'
batch = '*'
n_q = '*'
df_workers = load_analysis('outliers_workers', run, exp_name, batch)
worker_dicts = df_workers.to_dict('records')
df_pairs = load_analysis('pairs', run, exp_name, batch)
pair_dicts = df_pairs.to_dict('records')

data_dicts = load_experiment_data(run, exp_name, n_q, batch, remove_not_val = True)

df = get_pair_workers_df(pair_dicts, worker_dicts, data_dicts)
