from utils_data import load_experiment_data, load_config
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from utils_analysis import collect_contradictions, load_contradiction_pairs


from statistics import stdev
import argparse

def filter_with_stdv(workers, measure = 'contradiction_poss_contradiction_ratio', n_stds=1):
    cont_rate = [float(d[measure]) for d in workers]
    av_cont = sum(cont_rate)/len(cont_rate)
    std_cont = stdev(cont_rate)
    if measure == 'contradiction_poss_contradiction_ratio':
        thresh = (n_stds * std_cont) + av_cont
    elif measure == 'wqs':
        thresh = (n_stds * std_cont) - av_cont
    workers_to_remove = []
    for d in workers:
        score = float(d[measure])
        if measure == 'contradiction_poss_contradiction_ratio':
            if score > thresh:
                workers_to_remove.append(d['workerid'])
        elif measure == 'wqs':
            if score < thresh:
                workers_to_remove.append(d['workerid'])
    return workers_to_remove



def remove_contradicting_workers(all_annotations, dict_list_workers, unit,  n_stds):

    if unit == 'batch':
        annotations_by_unit = sort_by_key(all_annotations, ['filename','completionurl'])
        workers_by_unit = sort_by_key(dict_list_workers, ['filename-url'])
    elif unit == 'pair':
        annotations_by_unit = sort_by_key(all_annotations, ['property','concept'])
        workers_by_unit = sort_by_key(dict_list_workers, ['pair'])


    elif unit == 'total':
        annotations_by_unit = dict()
        annotations_by_unit['total'] = all_annotations
        workers_by_unit = dict()
        workers_by_unit['total'] = dict_list_workers
    clean_annotations = []

    for unit_id, workers in workers_by_unit.items():
        workers_to_remove = filter_with_stdv(workers,
                         measure = 'contradiction_poss_contradiction_ratio',
                         n_stds = n_stds)

        annotations = annotations_by_unit[unit_id]
        for d in annotations:
            worker = d['workerid']
            if worker not in workers_to_remove:
                clean_annotations.append(d)
            #if unit_id == 'dangerous-scalpel':
            #    print('remove:', worker in workers_to_remove, worker)
    return clean_annotations



def remove_contradictory_annotations(all_annotations):
    clean_annotations = []
    contradictions = load_contradiction_pairs()
    annotations_by_unit = sort_by_key(all_annotations, ['property','concept'])
    for pair, annotations in annotations_by_unit.items():
        annotations_per_worker = sort_by_key(all_annotations, ['workerid'])
        for w, annotations in annotations_per_worker.items():
            pair_contradictions = collect_contradictions(annotations, contradictions, threshold = 0)
            if len(pair_contradictions) == 0:
                clean_annotations.extend(annotations)

    return clean_annotations
    #workers_by_unit = sort_by_key(dict_list_workers, ['pair'])



def remove_low_quality_workers_ct(all_annotations, unit,  n_stds):
    ct_workers = load_ct('*', 'experiment*', '*', 'workers', as_dict=True)
    ct_by_workers = sort_by_key(ct_workers, ['worker'])

    if unit == 'batch':
        annotations_by_unit = sort_by_key(all_annotations, ['filename','completionurl'])
    elif unit == 'pair':
        annotations_by_unit = sort_by_key(all_annotations, ['property','concept'])
    elif unit == 'total':
        annotations_by_unit = dict()
        annotations_by_unit['total'] = all_annotations

    clean_annotations = []
    for unit_id, annotations in annotations_by_unit.items():
        worker_dicts = []
        workers = set([d['workerid'] for d in annotations])
        for w in workers:
            w_dict = dict()
            w_dict['workerid'] = w
            w_dict['wqs'] = ct_by_workers[w][0]['wqs']
            worker_dicts.append(w_dict)
        workers_to_remove = filter_with_stdv(worker_dicts,
                         measure = 'wqs',
                         n_stds = n_stds)
        for d in annotations:
            worker = d['workerid']
            if worker not in workers_to_remove:
                clean_annotations.append(d)

    return clean_annotations

def clean_workers(data_dict_list, run,  group,  batch, metric, unit, n_stds):
    if metric == 'contradictions':
        if unit == 'total':
            analysis_type = 'workers'
        else:
            analysis_type = f'workers_by_{unit}'
        dict_list_workers = load_analysis(analysis_type, run, group, batch,
                                            as_dict = True)
        data_dict_list_clean= remove_contradicting_workers(data_dict_list,
                                dict_list_workers, unit,  n_stds)
    elif metric == 'ct_wqs':
        data_dict_list_clean = remove_low_quality_workers_ct(data_dict_list, unit,  n_stds)

    elif metric == 'exclude_contradictory_annotations':
        data_dict_list_clean = remove_contradictory_annotations(data_dict_list)

    return data_dict_list_clean


def main():

    config_dict = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--metric", default='contradictions', type=str)
    parser.add_argument("--units", default=['total', 'batch', 'pair'],\
                        type=list, nargs='+')
    parser.add_argument("--stds", default=[0.5, 1, 1.5, 2],\
                        type=list, nargs='+')
    args = parser.parse_args()

    run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    group = config_dict['group']
    metric = args.metric
    units = args.units
    stds = args.stds

    data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)

    print('Metric:', metric)
    print('Units:', units)
    print('Number of standard deviations away from mean for cut-off:', stds)

    for unit in units:
        for n_stds in stds:
            data_dict_list_clean = clean_workers(data_dict_list, run,  group,\
                            batch, metric, unit, n_stds)
            n_total = len(data_dict_list)
            n_clean = len(data_dict_list_clean)
            percent_clean = n_clean / n_total
            print(unit, n_stds)
            print(n_total, n_clean, percent_clean)


if __name__ == '__main__':
    main()
