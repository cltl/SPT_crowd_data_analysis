from utils_analysis import load_analysis
import numpy as np
from collections import Counter
from statistics import stdev
import os

# check whether workers of pairs have tendencies

# calculate averages for contradiction type
# compare how far away a worker/pair is from this average



def get_contradiction_stats(df):
    contradiction_pairs = [c for c in df.columns if c.startswith('(')]
    contradiction_stats = dict()
    for cont_pair in contradiction_pairs:
        conts_normalized = []
        for ind, row in df.iterrows():
            # normalize number of contradictions by number of annotators
            n_conts = row[cont_pair]
            if np.isnan(n_conts):
                n_conts = 0
            poss_conts = row['n_possible_contradictions']
            if poss_conts != 0:
                conts_norm = n_conts/poss_conts
            else:
                conts_norm = 0
            conts_normalized.append(conts_norm)
        av = sum(conts_normalized)/len(conts_normalized)
        sd = stdev(conts_normalized)
        contradiction_stats[cont_pair] = dict()
        contradiction_stats[cont_pair]['av'] = av
        contradiction_stats[cont_pair]['sd'] = sd
    return contradiction_stats

def get_outliers(pair_row, contradiction_stats):
    cont_dicts = []
    contradiction_pairs = [k for k, v in pair_row.items() if k.startswith('(')]
    poss_conts = pair_row['n_possible_contradictions']
    for p in contradiction_pairs:
        n_conts = pair_row[p]
        if poss_conts != 0:
            conts_norm = n_conts/poss_conts
        else:
            conts_norm = 0
        if np.isnan(conts_norm):
            conts_norm = 0
        conts_av = contradiction_stats[p]['av']
        diff = conts_norm - conts_av
        conts_sd = contradiction_stats[p]['sd']
        if diff > conts_sd:
           # print('more contradictions than expected')
            #print(p, conts_per_worker)
            d = dict()
            d['contradiction'] = p
            d['conts_ratio'] = conts_norm
            d['n_conts'] = n_conts
            cont_dicts.append(d)
        else:
            continue
            #print('less contradictions than expected')
            #print(p, conts_per_worker)
    return cont_dicts


def collect_outliers(df):
    contradiction_stats = get_contradiction_stats(df)
    for ind, row in df.iterrows():
        outliers = get_outliers(row, contradiction_stats)
        outlier_cnt = Counter()
        for d in outliers:
            cont = d['contradiction']
            outlier_cnt[cont] += d['n_conts']
        sum_outliers = sum(outlier_cnt.values())
        #row['n_outliers'] = sum_outliers
        df.loc[ind,'n_outliers'] = sum_outliers
        n_conts = row['n_possible_contradictions']
        if n_conts != 0:
            df.loc[ind, 'outlier_contradiction_rate'] = sum_outliers/n_conts
        else:
            df.loc[ind, 'outlier_contradiction_rate'] = 0
        outlier_contradictions = [d['contradiction']for outlier in outliers]
        df.loc[ind, 'outlier_contradictions'] = ' '.join(set(outlier_contradictions))

def main():
    analysis_type = 'workers'
    run = '*'
    exp_name = 'experiment1'
    batch = '*'
    df = load_analysis(analysis_type, run, exp_name, batch)
    collect_outliers(df)
    dir_path = f'../analyses/outliers_{analysis_type}/'
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    new_path = f'{dir_path}run{run}-group_{exp_name}-batch{batch}.csv'.replace('*', '-all-')
    df.to_csv(new_path)
    print(f'Results written to: {new_path}')

if __name__ == '__main__':
    main()
