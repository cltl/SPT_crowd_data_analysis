from utils import get_id_mapping

def analyze_worker_ids(exp_path, remove_not_val = True, v = True):

    mapping_dict = get_id_mapping(exp_path, remove_not_val = True, v = v)
    if mapping_dict != None:
        print(f'Id in output but not summaries: {mapping_dict["out"]}')
        print(f'Id in summary but not output: {mapping_dict["summary"]}')


def main():
    run = 3
    batch = 7
    n_q = 70
    group = 'experiment1'
    exp_path = f'run{run}-group_{group}/qu{n_q}-s_qu{n_q}-batch{batch}.csv'
    analyze_worker_ids(exp_path, remove_not_val = True)

if __name__ == '__main__':
    main()
