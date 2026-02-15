import db_cons as fb
import DSlogger
from multiprocessing import Process, freeze_support


db_con = fb.db_connect()

pct_limit = 84.99
procs_to_utilize = {'other': 9, 'person': 1, 'vessels': 3}  # # number of processes that will be used for each data bach
chars_to_exclude = ' ,“.”-‘’()©&[]"_\/''@:+*%`;#'
sanctions_catgrs = ['LEGALENTITY', 'COMPANY', 'ORGANIZATION', 'PERSON', 'VESSEL']

cntp_match_upd = """BEGIN tran
                          ...
                        commit;"""

vessel_match_upd = """BEGIN tran
                            ...
                        commit;"""


def get_sanctions():
    qr = """select..."""

    return fb.QueryDB(qr, db_con)


def get_counterparties():
    sql = """select..."""

    return fb.QueryDB(sql, db_con)


def get_vessels():
    sql = """select..."""

    return fb.QueryDB(sql, db_con)


def generate_batches(batches_ls, sphere_df, sa_df, procs_num, batch_len):
    cnt = 0

    for i in range(0, procs_num):
        print('Batch range:', cnt, cnt + batch_len)

        if i == procs_num - 1:  # last loop
            batches_ls.append([sphere_df, sa_df[cnt:]])
        else:
            batches_ls.append([sphere_df, sa_df[cnt:cnt + batch_len]])

        cnt += batch_len


def generate_processes(arr, data_batches, procs_num, func_name):
    for i in range(0, procs_num):
        arr.append(Process(target=func_name, args=(data_batches[i][0].to_numpy(), data_batches[i][1].to_numpy(), pct_limit, 'p'+str(i+1)) ))


def apply_cntps_check(sa_name, sphere_name, sa_countries, sphere_country, sphere_id):
    if sphere_name is None:
        return [sphere_id, 0]

    if sphere_country not in sa_countries:
        return [sphere_id, 0]

    return get_proximity_match(sa_name, sphere_name, sphere_id)


def apply_vessels_check(sa_imos, sphere_imos, sphere_id):
    if sphere_imos is None:
        return [sphere_id, 0]

    return get_proximity_match(sa_imos, sphere_imos, sphere_id)


def get_proximity_match(name1, name2, id):
    sa_name_len = len(name1)
    compareLength = len(name2)
    rightIndex = 0
    leftIndex = 0
    difference = 0

    if sa_name_len > compareLength:
        compareLength = sa_name_len

    while leftIndex < compareLength:
        left_char = name1[leftIndex:leftIndex+1]
        right_char = name2[rightIndex:rightIndex+1]

        if left_char != right_char:
            if left_char == name2[rightIndex+1:rightIndex + 2]:
                rightIndex += 1
            elif right_char == name1[leftIndex+1:leftIndex + 2]:
                leftIndex += 1

            difference += 1

        leftIndex += 1
        rightIndex += 1

    return [id, 100 - (difference * 100 / compareLength)]


def check_cntps_for_sanctions(cntps_ls, sancts_ls, match_pct_limit, proc_name):
    cins = []
    cntp_countries = []
    legal_names = []
    dba_names = []
    local_names = []

    for item in cntps_ls:
        cins.append(item[0])
        cntp_countries.append(item[1])
        legal_names.append(item[2])
        dba_names.append(item[3])
        local_names.append(item[4])

    for e, s_row in enumerate(sancts_ls):
        entity_name = [s_row[1] for x in cntps_ls]
        entity_countries = [s_row[2] for x in cntps_ls]

        f1 = map(apply_cntps_check, entity_name, legal_names, entity_countries, cntp_countries, cins)
        f2 = map(apply_cntps_check, entity_name, dba_names, entity_countries, cntp_countries, cins)
        f3 = map(apply_cntps_check, entity_name, local_names, entity_countries, cntp_countries, cins)

        res = [r for r in zip(f1, f2, f3) if r[0][1] > match_pct_limit or r[1][1] > match_pct_limit or r[2][1] > match_pct_limit]
        # print('\n{}-{}'.format(proc_name, e))

        if len(res) > 0:
            for item in res:
                print('C:', item)

                if item[0][1] >= item[1][1] and item[0][1] >= item[2][1]:
                    print(cntp_match_upd.format(item[0][0], int(item[0][1]), 'legal name', s_row[0]))
                    fb.CmdDB(cntp_match_upd.format(item[0][0], int(item[0][1]), 'legal name', s_row[0]), db_con)
                elif item[1][1] >= item[0][1] and item[1][1] >= item[2][1]:
                    print(cntp_match_upd.format(item[1][0], int(item[1][1]), 'dba name', s_row[0]))
                    fb.CmdDB(cntp_match_upd.format(item[1][0], int(item[1][1]), 'dba name', s_row[0]), db_con)
                else:
                    print(cntp_match_upd.format(item[2][0], int(item[2][1]), 'local name', s_row[0]))
                    fb.CmdDB(cntp_match_upd.format(item[2][0], int(item[2][1]), 'local name', s_row[0]), db_con)


def check_vessels_for_sanctions(vessels_ls, sancts_ls, match_pct_limit, proc_name):
    imos = []
    vessel_imos = []
    vessel_names = []

    for item in vessels_ls:
        imos.append(item[0])
        vessel_imos.append(str(item[0]))
        vessel_names.append(item[1])

    for e, s_row in enumerate(sancts_ls):
        entity_imos = [s_row[4] for x in vessels_ls]

        f1 = map(apply_vessels_check, entity_imos, vessel_imos, imos)

        res = [r for r in f1 if r[1] > match_pct_limit]
        # print('\n{}-{}'.format(proc_name, e))

        if len(res) > 0:
            for item in res:
                print('V:', item)

                if item[1] == 100:
                    print(vessel_match_upd.format(item[0], int(item[1]), s_row[0]))
                    fb.CmdDB(vessel_match_upd.format(item[0], int(item[1]), s_row[0]), db_con)


def main():
    # # GET MAIN DATA SETS
    counterparties_df = get_counterparties()
    vessels_df = get_vessels()
    sanctions_df = get_sanctions()

    # # SPLIT DATA SETS INTO CATEGORIES
    processes = []
    cntp_batches_ls = []
    vessel_batches_ls = []

    sphere_comps_df = counterparties_df[counterparties_df['cntrpart_type'].isin(['LEGAL ENTITY']) |
                                        (counterparties_df['cntrpart_type'].isin(['INDIVIDUAL']) & counterparties_df['doing_buss_as'].notna()) ]
    sphere_indiv_df = counterparties_df[counterparties_df['cntrpart_type'].isin(['INDIVIDUAL'])]

    san_comps_df = sanctions_df[sanctions_df['entity_type'].isin([sanctions_catgrs[0], sanctions_catgrs[1], sanctions_catgrs[2]])]
    comps_batch_max_len = int(len(san_comps_df) / procs_to_utilize['other'])

    san_pers_df = sanctions_df[sanctions_df['entity_type'].isin([sanctions_catgrs[3]])]
    pers_batch_max_len = int(len(san_pers_df) / procs_to_utilize['person'])

    san_vessels_df = sanctions_df[sanctions_df['entity_type'].isin([sanctions_catgrs[4]])]
    vessels_batch_max_len = int(len(san_vessels_df) / procs_to_utilize['vessels'])

    # # SPLIT SANCTIONS DATAFRAME INTO SMALLER BATCHES WHICH WILL BE ASSIGNED TO EACH PROCESS GENERATED BELOW
    generate_batches(cntp_batches_ls, sphere_comps_df, san_comps_df, procs_to_utilize['other'], comps_batch_max_len)
    generate_batches(cntp_batches_ls, sphere_indiv_df, san_pers_df, procs_to_utilize['person'], pers_batch_max_len)
    generate_batches(vessel_batches_ls, vessels_df, san_vessels_df, procs_to_utilize['vessels'], vessels_batch_max_len)

    # # APPLY MULTI PROCESSING FOR EACH DATA BATCH CREATED
    generate_processes(processes, cntp_batches_ls, procs_to_utilize['other'] + procs_to_utilize['person'], check_cntps_for_sanctions)
    generate_processes(processes, vessel_batches_ls, procs_to_utilize['vessels'], check_vessels_for_sanctions)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    for p in processes:
        p.close()

    db_con.close()


if __name__ == "__main__":
    freeze_support()

    try:
        main()
    except Exception as ex:
        print('Exception: ', str(ex))
        DSlogger.sanctions_logger('ERROR', [__file__, 'main()', str(ex)])

