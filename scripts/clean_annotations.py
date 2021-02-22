from utils_data import load_experiment_data, load_config, annotations_to_file
from utils_analysis import sort_by_key
from utils_analysis import load_analysis, load_ct
from utils_analysis import collect_contradictions, load_contradiction_pairs


from statistics import stdev
import argparse
import os
import csv

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
    print('all runs before cleaning')
    all_runs = set([d['f_name_full'].split('/')[3] for d in all_annotations])
    print(all_runs)
    if unit == 'batch':
        annotations_by_unit = sort_by_key(all_annotations, ['filename','completionurl'])
        workers_by_unit = sort_by_key(dict_list_workers, ['filename-url'])
        print('annotations_by_unit', len(annotations_by_unit))
        print('workers by unit', len(workers_by_unit))
        print('comparing unit ids')
        for u, annotations in annotations_by_unit.items():
            if u in workers_by_unit:
                continue
            else:
                print('not found', u)
    elif unit == 'pair':
        annotations_by_unit = sort_by_key(all_annotations, ['property','concept'])
        workers_by_unit = sort_by_key(dict_list_workers, ['pair'])


    elif unit == 'total':
        annotations_by_unit = dict()
        annotations_by_unit['total'] = all_annotations
        workers_by_unit = dict()
        workers_by_unit['total'] = dict_list_workers
    clean_annotations = []
    print('removing workers')
    for unit_id, workers in workers_by_unit.items():
        workers_to_remove = filter_with_stdv(workers,
                         measure = 'contradiction_poss_contradiction_ratio',
                         n_stds = n_stds)
        #print(unit_id, len(workers_to_remove))

        annotations = annotations_by_unit[unit_id]
        for d in annotations:
            worker = d['workerid']

            if worker not in workers_to_remove:
                clean_annotations.append(d)
            #if unit_id == 'dangerous-scalpel':
            #    print('remove:', worker in workers_to_remove, worker)
    return clean_annotations


def time_filter(all_annotations, n_std, direction = 'both',):
    clean_annotations = []
    annotations_by_batch = sort_by_key(all_annotations, ['completionurl', 'f_name_full'])
    for batch, annotations in annotations_by_batch.items():
        annotations_by_worker = sort_by_key(annotations, ['workerid'])
        worker_seconds = dict()
        for worker, annotations in annotations_by_worker.items():
            seconds = [float(a['duration_in_seconds']) for a in annotations
                       if not a['duration_in_seconds'] is None]
            worker_seconds[worker] = sum(seconds)
        mean_seconds = sum(worker_seconds.values())/len(worker_seconds)
        std = stdev(worker_seconds.values())
        for worker, seconds in worker_seconds.items():
            if (direction == 'below') and (seconds < mean_seconds - n_std*std):
                print('worker took too little time', worker)
            elif  (direction == 'above') and (seconds > mean_seconds + n_std*std):
                print('worker took too much time', worker)
            elif (direction == 'both') and ((seconds > mean_seconds + n_std*std) or (seconds < mean_seconds - n_std*std)):
                print('worker outside 2 stdevs', worker)
            else:
                annotations = annotations_by_worker[worker]
                clean_annotations.extend(annotations)
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

def clean_workers(data_dict_list, runs,  groups,  batch, metric, unit, n_stds):
    if metric == 'contradictions':
        if unit == 'total':
            analysis_type = 'workers'
        else:
            analysis_type = f'workers_by_{unit}'
        dict_list_workers = load_analysis(analysis_type, runs, groups, batch, as_dict = True)
        print('worker analysis', type(dict_list_workers), len(dict_list_workers))
        data_dict_list_clean= remove_contradicting_workers(data_dict_list,
                                dict_list_workers, unit,  n_stds)
        print(len(data_dict_list_clean))
    elif metric == 'ct_wqs':
        data_dict_list_clean = remove_low_quality_workers_ct(data_dict_list, unit,  n_stds)

    elif metric == 'exclude_contradictory_annotations':
        data_dict_list_clean = remove_contradictory_annotations(data_dict_list)

    elif metric == 'time-below':
        data_dict_list_clean = time_filter(data_dict_list, n_stds, direction = 'below')

    return data_dict_list_clean


def main():

    config_dict = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--metric", default='contradictions', type=str)
    parser.add_argument("--units", default=['total', 'batch', 'pair'],
                        type=list, nargs='+')
    parser.add_argument("--stds", default=[0.5, 1, 1.5, 2],
                        type=list, nargs='+')
    args = parser.parse_args()

    #run = config_dict['run']
    batch = config_dict['batch']
    n_q = config_dict['number_questions']
    #group = config_dict['group']
    metric = args.metric
    units = args.units
    stds = args.stds

    #data_dict_list = load_experiment_data(run, group, n_q, batch, remove_not_val = True)

    #print('Metric:', metric)
    #print('Units:', units)
    #print('Number of standard deviations away from mean for cut-off:', stds)

    units = ['batch', 'pair', 'total']
    #metric = 'contradictions'
    metric = 'time-below'
    n_stds = [0.5, 1, 1.5, 2]
    if metric == 'time_below':
        units = ['batch']
    clean = True

    configs = [
        #('1', 'experiment1')
       ('3', 'experiment1'),
        ('4', 'experiment2')
        #('5_pilot', 'experiment3'),
        #('5_scalar_heat', 'scalar_heat')
    ]
    batch = '*'
    n_q = '*'
    n_lists = '*'

    runs = [conf[0] for conf in configs]
    groups = [conf[1] for conf in configs]

    # make dir
    all_data = []
    for run, group in configs:
        data = load_experiment_data(run, group, n_q, n_lists,
                         batch, verbose = False)
        print(data[0].keys())
        print(data[0]['f_name_full'])
        all_data.extend(data)

    if clean:
        # clean all data
        for unit in units:
            for n_std in n_stds:
                print(len(all_data), type(all_data))
                data_clean = clean_workers(all_data, runs, groups, batch, metric, unit, n_std)
                #data_clean = (all_data, runs, groups, batch, metric, unit, n_std)
                print(type(data_clean), len(data_clean))
                print(data_clean[0].keys())
                all_runs =set( [d['f_name_full'].split('/')[3] for d in data_clean])
                print('all runs after cleaning')
                print(all_runs)
                name = f'clean_{metric}_{unit}_{n_std}'
                name_dir = f'annotations_{name}'
                data_by_filepath = sort_by_key(data_clean, ['f_name_full'])
                for f, data in data_by_filepath.items():
                    new_f = f.replace('prolific_output', name_dir)
                    fbase = os.path.basename(new_f)
                    dir_path = new_f.rstrip(fbase)
                    if not os.path.isdir(dir_path):
                        os.makedirs(dir_path)
                    header = data[0].keys()
                    with open(new_f, 'w') as outfile:
                        writer = csv.DictWriter(outfile, fieldnames = header)
                        writer.writeheader()
                        for d in data:
                            writer.writerow(d)
    else:
        data_clean = all_data
        print(type(data_clean), len(data_clean))
        print(data_clean[0].keys())
        all_runs = set([d['f_name_full'].split('/')[3] for d in data_clean])
        print('all runs after cleaning')
        print(all_runs)
        name = f'data_processed'
        name_dir = f'annotations_{name}'
        data_by_filepath = sort_by_key(data_clean, ['f_name_full'])
        for f, data in data_by_filepath.items():
            new_f = f.replace('prolific_output', name_dir)
            fbase = os.path.basename(new_f)
            dir_path = new_f.rstrip(fbase)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
            header = data[0].keys()
            with open(new_f, 'w') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=header)
                writer.writeheader()
                for d in data:
                    writer.writerow(d)






if __name__ == '__main__':
    main()
