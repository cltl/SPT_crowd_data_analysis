from utils_analysis import load_analysis




def get_whitelist():
    run = '*'
    exp_name = 'experiment*'
    batch = '*'

    analysis_type = 'workers'
    whitelist = set()
    df = load_analysis(analysis_type, run, exp_name, batch)

    for ind, row in df.iterrows():
        n_contradictions = row['n_contradictions']
        n_fails = row['n_fails']
        n_annotations = row['n_annotations']
        ratio = row['contradiction_poss_contradiction_ratio']
        #if n_annotations > 30 and n_fails < 3 and n_contradictions < 3:
        if ratio < 0.05 and n_annotations > 30 and n_fails < 3:
            whitelist.add(row['workerid'])

    print(f'# whitelist workers: {len(whitelist)}')
    whitelist_set = set(whitelist)
    return whitelist_set

def load_existing_whitelist():
    with open('../worker_interaction/whitelist.txt') as infile:
        existing_whitelist_set = set(infile.read().strip().split(','))

    return existing_whitelist_set


def whitelist_to_file(updated_whitelist):
    path = '../worker_interaction/whitelist.txt'
    with open(path, 'w') as outfile:
        outfile.write(','.join(updated_whitelist))
    print(f'updated whitelist written to: {path}')


def main():
    whitelist_set = get_whitelist()
    print(len(whitelist_set))
    existing_whitelist_set = load_existing_whitelist()
    print(len(existing_whitelist_set))
    if len(existing_whitelist_set) > 2:
        updated_whitelist = existing_whitelist_set.intersection(whitelist_set)
    else:
        updated_whitelist = whitelist_set
    print(len(updated_whitelist))

    #print(whitelist_set)
    print('---')
    #print(existing_whitelist_set)
    print('---')
    #print(updated_whitelist)
    whitelist_to_file(updated_whitelist)
    
    # reset whitelist:
    #whitelist_to_file(whitelist_set)




if __name__ == '__main__':
    main()
