####################
# main.py
# version 1.0.0
# Python 3.6.2
####################

import copy
import time

from scripts.data_processor import GSProcessor


def main():

    ########################
    # list databses
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # create list to store information on source queries
    src_inputs = ['str', 'source_id', 'source_domain', None, None]
    source_queries = [['wildcard_match', '%', src_inputs],
                      ['cswm', '%', src_inputs],
                      ['cswm_child', '%', src_inputs],
                      ['cswm_desc', '%', src_inputs],
                      ['exact_match', '', src_inputs],
                      ['csem', '', src_inputs],
                      ['csem_child', '', src_inputs],
                      ['csem_desc', '', src_inputs]]

    # create list to store information on standard queries
    std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', 'code_count', databases]
    standard_queries = [['stand_terms', '', std_inputs],
                        ['stand_terms_child', '', std_inputs],
                        ['stand_terms_desc', '', std_inputs]]

    # list sheets to process
    sheets = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
              'Steroid-Induced Osteonecrosis_155', 'Systemic Lupus Erythematosus_1058']

    # loop over data sets
    for sht in sheets[:2]:
        print('\n', '*' * 25, 'Processing Phenotype: {0}'.format(sht), '*' * 25, '\n')

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

        # group data for processing
        data_groups = data.groupby(['source_domain', 'input_type', 'standard_vocabulary'])

        for x in [x for x in data_groups.groups if 'String' in x[1]]:
            print(x)
            grp_data = data_groups.get_group(x)

            print('\n', '_' * 10, 'Running Source Query: {0}'.format(x[2]), '_' * 10, '\n')

            for src_query in source_queries:
                print('\n', '=' * 25, 'Running Source Query: {0}'.format(src_query[0]), '=' * 25, '\n')

                # run standard query
                all_data.set_data(grp_data)
                source_results = all_data.regular_query(src_query, 'sandbox-tc', url, databases[0])

                if len(source_results) != 0:

                    # write out source data to Google sheet
                    spreadsheet_name = '{0}_{1}_{2}'.format(sht.split('_')[0].upper(), x[0].upper(), src_query[0])
                    tab_name = '_'.join(spreadsheet_name.split('_')[2:])

                    # order columns
                    src_results = source_results.copy()
                    src_results = src_results[['source_string', 'source_code', 'source_name', 'source_vocabulary']]

                    # create new spreadsheet class
                    all_data.create_spreadsheet(spreadsheet_name, 'callahantiff@gmail.com')
                    temp_data = GSProcessor([spreadsheet_name, tab_name])
                    temp_data.create_worksheet(tab_name)
                    temp_data.set_worksheet(tab_name)
                    temp_data.sheet_writer(temp_data, src_results)

                    # pause to avoid over whelming the API
                    time.sleep(10)

                    # process second half of queries -- getting standard codes
                    for std_query in standard_queries:
                        print('\n', '=' * 25, 'Running Standard Query: {0}'.format(std_query[0]), '=' * 25, '\n')

                        # copy query list before inserting a new list into it
                        std_query_mod = copy.deepcopy(std_query)
                        std_query_mod[2].insert(len(std_query_mod[2]), x[2])

                        # run query
                        all_data.set_data(source_results)
                        std_results = all_data.regular_query(std_query_mod, 'sandbox-tc', url, databases[0])

                        if len(std_results) != 0:

                            # order columns
                            stand_results = std_results[['source_string', 'source_code', 'source_name',
                                                         'source_vocabulary', 'standard_code', 'standard_name',
                                                         'standard_vocabulary', 'CHCO_count', 'MIMICIII_count']]

                            # make sure connection is still live before writing out data
                            new_tab = '{0}_{1}'.format(tab_name, std_query[0])
                            temp_data = GSProcessor([spreadsheet_name, tab_name])
                            temp_data.create_worksheet(new_tab)
                            temp_data.set_worksheet(new_tab)
                            temp_data.sheet_writer(temp_data, stand_results)

    # ########################
    # # GBQ: create a new table -- after verifying mappings
    # db_kids = GBQ('sandbox-tc', 'CHCO_DeID_Oct2018')
    # # db_mimic = GBQ("sandbox-tc", "CHCO_DeID_Oct2018")
    # # tables = db.table_info()
    #
    # # create a new table
    # table_name = "ADHD_"
    # db.create_table(data, table_name)

#
# if __name__ == '__main__':
#     with auger.magic([GSProcessor], verbose=True):  # this is the new line and invokes Auger
#         main()
