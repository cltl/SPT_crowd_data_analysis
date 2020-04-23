# generate correct contradiction pairs
import csv
from collections import defaultdict

def load_relations():
    with open('../scheme/relation_overview_run3.csv') as infile:
        dict_list = list(csv.DictReader(infile))
    level_dict = defaultdict(set)
    for d in dict_list:
        level_dict[d['level']].add(d['relation'])
    return level_dict

def gen_conts():
    level_dict = load_relations()
    # we do not want contradictions between all and few
    rel_all = level_dict['all']
    rel_few = level_dict['few']
    pairs = []

    for r1 in rel_all:
        for r2 in rel_few:
            pair = set([r1, r2])
            if pair not in pairs:
                pairs.append(pair)
    conts = [tuple(sorted(pair)) for pair in pairs]

    with open('../scheme/contradictions.csv', 'w') as outfile:
        for c in conts:
            outfile.write(','.join(c)+'\n')
    print('Contradictions written to: ../scheme/contradictions.csv')
    return conts

if __name__ == '__main__':

    conts = gen_conts()
