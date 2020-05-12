from utils_analysis import load_analysis

analysis_type = 'workers'
run = '*'
exp_name = 'experiment*'
batch = '*'

whitelist = set()
df = load_analysis(analysis_type, run, exp_name, batch)


for ind, row in df.iterrows():
    n_contradictions = row['n_contradictions']
    n_fails = row['n_fails']
    n_annotations = row['n_annotations']
    if n_annotations > 30 and n_fails < 2 and n_contradictions <3:
        whitelist.add(row['workerid'])

print(f'# whitelist workers: {len(whitelist)}')
print(','.join(whitelist))
