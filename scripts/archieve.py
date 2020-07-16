def comparison_matching_pairs(name1, name2, df1, df2):

    # get overlapping pairs
    pairs_df1 = set([row['pair'] for ind, row in df1.iterrows()])
    pairs_df2 = set([row['pair'] for ind, row in df2.iterrows()])
    shared_pairs = pairs_df1.intersection(pairs_df2)

    rows_df1_clean = [row for ind, row in df1.iterrows() if row['pair'] in shared_pairs]
    rows_df2_clean = [row for ind, row in df2.iterrows() if row['pair'] in shared_pairs]

    df1_clean = pd.DataFrame(rows_df1_clean)
    df2_clean = pd.DataFrame(rows_df2_clean)

    #df_add_row = df_merge_col.append(add_row, ignore_index=True)
    ratio1, ratio2 = comparison_general(name1, name2, df1_clean, df2_clean)
    print(f'This analysis only includes pairs annotated in run {name1} and run {name2}.')
    return ratio1, ratio2

def compare_runs(name1, name2, df1, df2, comp = 'all'):
    if comp == 'all':
        r1, r2 = comparison_general(name1, name2, df1, df2)
    elif comp == 'pairs':
        r1, r2 = comparison_matching_pairs(name1, name2, df1, df2)
    return r1, r2

def comparison_general(name1, name2, df1, df2):

    ratio1 = get_ratio_contradicting_pair_annotations(df1)
    ratio2 = get_ratio_contradicting_pair_annotations(df2)

    print(f'Set {name1} as a contradiction ratio of {ratio1}')
    print(f'Set {name2} as a contradiction ratio of {ratio2}')
    print('The ratio is based on the number of workers annotating a pair.')
    print('A worker always annotates a full set.')
    return ratio1, ratio2

def show_pairs_of_worker(worker, df):
    print(f'Worker {worker} contradicted themselves in the following pairs:')
    print()
    for ind, row in df.iterrows():
        workers_cont = row['workers_contradicting'].split(' ')
        if worker in workers_cont:
            pair = row['pair']
            print(f'{pair} \t total workers contradicting themselves: {len(workers_cont)}')



def get_ratio_contradicting_pair_annotations(df):

    n_worker_pairs_total = 0.0
    n_worker_pairs_contradicting = 0.0

    for ind, row in df.iterrows():
        n_worker_pairs_total += row['n_workers']
        n_worker_pairs_contradicting += row['n_workers_contradicting']

    if n_worker_pairs_contradicting != 0:
        ratio = n_worker_pairs_contradicting / n_worker_pairs_total
    else:
        ratio = 0.0
    return ratio

def rest_of_old_aggretation():
    prop_rels = defaultdict(list)
    ct_rels = defaultdict(dict)
    triple_dicts = []
    for rel, data in data_by_rel.items():
        answers = [d['answer'] for d in data]
        true_cnt = answers.count('true')
        prop = true_cnt/len(answers)
        prop_rels[prop].append(rel)
        majority_vote = False
        if prop > 0.5:
            majority_vote = True
        #print(rel, pair, majority_vote)
        triple_dict = dict()
        triple_dict['relation'] = rel.strip()
        triple_dict['workerid'] = 'aggregated'
        #triple_dict['level'] = rel_level_mapping[rel]
        triple_dict['quid'] = data[0]['quid']
        triple_dict['property'] = pair.split('-')[0]
        triple_dict['concept'] = pair.split('-')[1]
        triple_dict['majority_vote'] = majority_vote
        triple_dict['completionurl'] = 'aggregated'
        # Get crowd truth scores
        triple = f'{rel}-{pair}'
        quid = data[0]['quid']
        for ct_thresh in ct_thresholds:
            ct_score = get_ua_score(quid, units_by_quid)
            if ct_score in ct_rels[ct_thresh].keys():
                ct_rels[ct_thresh][ct_score].append(rel)
            else:
                ct_rels[ct_thresh][ct_score] = [rel]
            if ct_score > ct_thresh:
                ct_vote = True
            else:
                ct_vote = False
            triple_dict[f'ct_vote_{ct_thresh}'] = ct_vote
        triple_dicts.append(triple_dict)
    # add top label
    top_prop = max(prop_rels.keys())
    for d in triple_dicts:
        rel = d['relation']
        if rel in prop_rels[top_prop]:
            d['top_vote'] = True
        else:
            d['top_vote'] = False

    for ct_thresh in ct_thresholds:
        top_ct = max(ct_rels[ct_thresh].keys())
        for d in triple_dicts:
            rel = d['relation']
            if rel in ct_rels[ct_thresh][top_ct]:
                d[f'top_vote_ct_{ct_thresh}'] = True
            else:
                d[f'top_vote_ct_{ct_thresh}'] = False

    aggregated_binary_labels.extend(triple_dicts)
