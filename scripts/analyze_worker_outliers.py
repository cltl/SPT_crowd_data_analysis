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
        n_conts = []
        for ind, row in df.iterrows():
            conts = row[cont_pair]
            if np.isnan(conts):
                conts = 0.0
            n_conts.append(conts)
        av = sum(n_conts)/len(df)
        sd = stdev(n_conts)
        contradiction_stats[cont_pair] = dict()
        contradiction_stats[cont_pair]['av'] = av
        contradiction_stats[cont_pair]['sd'] = sd
    return contradiction_stats

def get_outliers(row, contradiction_stats):
    cont_dicts = []
    contradiction_pairs = [k for k, v in row.items() if k.startswith('(')]
    for p in contradiction_pairs:
        n_conts = row[p]
        conts_av = contradiction_stats[p]['av']
        diff = n_conts - conts_av
        conts_sd = contradiction_stats[p]['sd']
        if n_conts > conts_av + conts_sd:
            d = dict()
            d['contradiction'] = p
            d['n_conts'] = n_conts
            cont_dicts.append(d)
    return cont_dicts


def collect_outliers(df):
    contradiction_stats = get_contradiction_stats(df)
    #for pair, stats in contradiction_stats.items():
    #    print(pair)
    #    print(stats['av'], stats['sd'])
    for ind, row in df.iterrows():
        outliers = get_outliers(row, contradiction_stats)
        outlier_cnt = Counter()
        for d in outliers:
            cont = d['contradiction']
            outlier_cnt[cont] += d['n_conts']
        sum_outliers = sum(outlier_cnt.values())
        df.loc[ind,'n_outliers'] = sum_outliers
        n_conts = row['n_possible_contradictions']
        if n_conts != 0:
            df.loc[ind, 'outlier_contradiction_rate'] = sum_outliers/n_conts
        else:
            df.loc[ind, 'outlier_contradiction_rate'] = 0
        outlier_contradictions = [d['contradiction']for outlier in outliers]
        df.loc[ind, 'outlier_contradictions'] = ' '.join(set(outlier_contradictions))

def get_worker_contradiction_outlier_analysis(analysis_type, run, exp_name, batch):
    df = load_analysis(analysis_type, run, exp_name, batch)
    collect_outliers(df)
    dir_path = f'../analyses/{analysis_type}-outliers/'
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    new_path = f'{dir_path}run{run}-group_{exp_name}-batch{batch}.csv'.replace('*', '-all-')
    df.to_csv(new_path)
    print(f'Results written to: {new_path}')
    return df

def main():
    analysis_type = 'workers'
    run = '4'
    exp_name = 'experiment2'
    batch = '*'
    get_worker_contradiction_outlier_analysis(analysis_type, run, exp_name, batch)



if __name__ == '__main__':
    main()
