from calculate_iaa import get_alpha
from statistics import stdev
from load_data import load_experiment_data
from utils_analysis import sort_by_key
from utils_analysis import load_analysis

def filter_with_stdv(workers, measure = 'contradiction_poss_contradiction_ratio', n_stds=1):
    cont_rate = [float(d[measure]) for d in workers]
    av_cont = sum(cont_rate)/len(cont_rate)
    std_cont = stdev(cont_rate)
    thresh = (n_stds * std_cont) + av_cont
    workers_to_remove = []
    for d in workers:
        cont_rate = float(d[measure])
        if cont_rate > thresh:
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

def clean_worker_cont_rate(data_dict_list, run,  group,  batch, unit, n_stds):
    if unit == 'total':
        analysis_type = 'workers'
    else:
        analysis_type = f'workers_by_{unit}'
    dict_list_workers = load_analysis(analysis_type, run, group, batch, as_dict = True)
    data_dict_list_clean= remove_contradicting_workers(data_dict_list, dict_list_workers, unit,  n_stds)
    return data_dict_list_clean


def main():
    run = '*'
    group = 'experiment*'
    n_q = '*'
    batch = '*'

    #load_analysis(analysis_type, run, exp_name, batch)

    #n_stds = 3
    all_annotations = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    print('IAA raw')
    iaa = get_alpha(all_annotations)
    iaa_levels = get_alpha(all_annotations, collapse_relations = 'levels')
    print()

    units = ['total', 'batch', 'pair']
    #units = ['pair']
    stds = [0.5, 1, 1.5, 2, 2.5, 3]
    #stds = [1]


    for unit in units:
        for n_stds in stds:
            clean_annotations = clean_worker_cont_rate(all_annotations,\
                                    run,  group,  batch, unit, n_stds)

            n_total = len(all_annotations)
            n_clean = len(clean_annotations)
            percent_clean = n_clean / n_total
            iaa_alpha = get_alpha(clean_annotations)
            iaa_alpha_levels = get_alpha(clean_annotations, collapse_relations = 'levels')
            print(unit, n_stds)
            print(n_total, n_clean, percent_clean)
            print(iaa_alpha, iaa_alpha_levels)
            print()



if __name__ == '__main__':
    main()
