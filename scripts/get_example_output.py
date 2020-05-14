def get_answer_dicts(data_dict_list, test_pair):
    data_by_pairs = sort_by_key(data_dict_list, ['property', 'concept'])
    test_annotations = data_by_pairs[test_pair]
    data_by_relations = sort_by_key(test_annotations, ['relation'])
    relations = ['typical_of_property', 'typical_of_concept',
                 'affording_activity', 'implied_category', 'variability_limited',
                 'variability_open', 'rare', 'unusual', 'impossible', 'creative']
    answer_dicts = []
    for rel in relations:
        data =data_by_relations[rel]
        cnt = 0
        total = 0
        an_dict = dict()
        for d in data:
            total += 1
            if d['answer'] == 'true':
                cnt += 1
        an_dict['relation'] = rel
        an_dict[test_pair] = cnt
        answer_dicts.append(an_dict)
    return answer_dicts

test_pairs  = ['sweet-honey', 'made_of_wood-beam', 'hot-chutney']

all_dicts = []
for test_pair in test_pairs:
    answer_dicts = get_answer_dicts(data_dict_list, test_pair)
    all_dicts.append(answer_dicts)

all_rows = []
for i in range(len(all_dicts[0])):
    row_dict = dict()
    for n, d in enumerate(all_dicts):
        d_in_position = all_dicts[n][i]
        row_dict.update(d_in_position)
    all_rows.append(row_dict)

row_df = pd.DataFrame(all_rows)
row_df = row_df[['relation', 'sweet-honey', 'made_of_wood-beam', 'hot-chutney']]
row_df.to_csv('example_annotatios.csv', index=False)
