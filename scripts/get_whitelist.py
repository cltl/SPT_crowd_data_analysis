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
        if n_annotations > 30 and n_fails < 2 and n_contradictions <3:
            whitelist.add(row['workerid'])

    print(f'# whitelist workers: {len(whitelist)}')
    whitelist_set = set(whitelist)
    return whitelist_set

def load_existing_whitelist():
    with open('../worker_interaction/whitelist.txt') as infile:
        existing_whitelist_set = set(infile.read().split(', '))

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
    updated_whitelist = existing_whitelist_set.intersection(whitelist_set)
    whitelist_to_file(updated_whitelist)




if __name__ == '__main__':
    main()
