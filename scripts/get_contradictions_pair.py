from collections import defaultdict

from utils import load_experiment_data
from utils import parse_answer
from utils import get_pair_dict
from utils import load_contradiction_pairs
from utils import get_relation_counts
from utils import consistency_check





if __name__ == '__main__':
    run = 3
    batch = 0
    n_q = 70
    group = 'experiment1'
    dict_list_out = load_experiment_data(run, group, n_q, batch, remove_not_val = True)
    pair_dict = get_pair_dict(dict_list_out)
    contradiction_pairs = load_contradiction_pairs()
    for pair, relation_vecs in pair_dict.items():
        relation_counts = get_relation_counts(relation_vecs, normalize = True)
        contradictions = consistency_check(contradiction_pairs, relation_counts, thresh = 0.2)
        print(pair, len(contradictions)) #, contradictions)
        for rel, cnt in relation_counts.items():
            print(rel, cnt)
        print()
