####################
# main.py
# Python 3.6.2
####################

import copy
import time

from scripts.data_processor import GSProcessor


def standard_queries(data_class, data, queries, url, database, standard_vocab, spreadsheet_name):
    """Processes a pandas dataframe containing OMOP source concept_codes by mapping these codes to concept_ids in a
    standard OMOP vocabulary. These mapped codes are then written to a Google sheet via the GoogleDrive API.

    Args:
        data_class: A GoogleSheets class instance.
        data: A pandas dataframe.
        queries: A list of standard queries.
        url: A dictionary where keys are query names and values are url strings.
        database: A string that points to a Google BigQuery database.
        standard_vocab: A string that contains the name of a OMOP standard vocabulary to map source codes to.
        spreadsheet_name: A string containing the name of a GoogleSheet spreadsheet.

    Returns:
        None.
    """

    # process second half of queries -- getting standard codes
    for std_query in queries:

        print('\n', '=' * 25, 'Running Standard Query: {0}'.format(std_query[0]), '=' * 25, '\n')

        # set instance data
        data_class.set_data(data)
        print('len of source data before getting standard codes is {}'.format(len(data_class.get_data())))

        # copy query list before inserting a new list into it
        std_query_mod = copy.deepcopy(std_query)
        std_query_mod[2].insert(len(std_query_mod[2]), standard_vocab[2])

        # run query
        std_results = data_class.regular_query(std_query_mod, 'sandbox-tc', url, database)

        # make sure the query returned valid results
        counts = len(set(list(std_results['CHCO_count']) + list(std_results['MIMICIII_count'])))

        if len(std_results) != 0 and counts > 1:

            # order columns
            std_results = std_results[['source_string', 'source_code', 'source_name', 'source_vocabulary',
                                       'standard_code', 'standard_name', 'standard_vocabulary', 'CHCO_count',
                                       'MIMICIII_count']]

            # order rows
            std_results = std_results.sort_values(by=['source_string', 'source_code', 'source_vocabulary',
                                                      'standard_code', 'standard_vocabulary'], ascending=True)

            # write out results
            tab_name = '{0}_{1}'.format(spreadsheet_name[1], std_query[0])
            data_class.write_data(spreadsheet_name[0], tab_name, std_results)

    return None


def src_queries(data_class, data, url, database, queries, standard_vocab, spreadsheet):
    """Runs a list of queries against a Google BigQuery database designed to map a set of source strings or codes to
    OMOP concept_codes. The results are returned as a pandas dataframe. These results are then passed to a second
    function which maps the OMOP source concept_codes to concept_ids in a standard OMOP vocabulary. Finally,
    the results are written to a Google sheet via the GoogleDrive API.

    Args:
        data_class: A GoogleSheets class instance.
        data: A pandas dataframe.
        url: A dictionary where keys are query names and values are url strings.
        database: A string that points to a Google BigQuery database.
        queries: A list of query lists. The function assumes that this list contains three lists: 1) wildcard match,
        exact match, and standard.
        standard_vocab: A string that contains the name of a OMOP standard vocabulary to map source codes to.
        spreadsheet: .

    Returns:
        None.
    """

    for query in queries[:-1]:
        print('\n', '=' * 25, 'Running Source Query: {0}'.format(query[0]), '=' * 25, '\n')

        # set data
        data_class.set_data(data)

        print('len of input data is {}'.format(len(data)))

        src_results = data_class.regular_query(query, 'sandbox-tc', url, database)

        # make sure the query returned valid results
        if len(src_results) != 0:

            # order columns and rows
            source_res_cpy = src_results.copy()
            source_res_cpy = source_res_cpy[['source_string', 'source_code', 'source_name', 'source_vocabulary']]
            source_res_cpy = source_res_cpy.sort_values(by=['source_string', 'source_code', 'source_vocabulary'],
                                                        ascending=True)

            print('len of source data results is {}'.format(len(data_class.get_data())))

            # check data dimensions to ensure we can write data
            data_set_size = len(source_res_cpy) * len(list(source_res_cpy))

            if spreadsheet in {sheet.title: sheet.id for sheet in data_class.client.openall()}.keys():

                if data_class.count_spreadsheet_cells(spreadsheet) + data_set_size < 5000000:

                    # write out results
                    tab_name = '{0}'.format(query[0])
                    data_class.write_data(spreadsheet, tab_name, source_res_cpy)

                else:
                    # write out results to a new spreadsheet named after the query
                    new_sheet = '{0}_{1}'.format('_'.join(spreadsheet.split('_')[0:2]), query[0])
                    tab_name = '{0}'.format('_'.join(spreadsheet.split('_')[2:]))
                    data_class.write_data(new_sheet, tab_name, source_res_cpy)

            else:
                # when spreadsheet does not yet exist -- write out results
                tab_name = '{0}'.format(query[0])
                data_class.write_data(spreadsheet, tab_name, source_res_cpy)

            # run standard queries and write results
            src_spreadsheet = [spreadsheet, tab_name]
            standard_queries(data_class, src_results, queries[-1], url, database, standard_vocab, src_spreadsheet)

            # sleep system and re-authorize API client
            time.sleep(10)
            data_class.authorize_client()

    return None


def main():

    ########################
    # list databases
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # QUERY ARGUMENTS
    # queries that map input strings to source_codes
    src_inputs = ['str', 'source_id', 'source_domain']
    wild = [[['wildcard_match', '%', src_inputs], ['wildcard_match_child', '%', src_inputs],
            ['wildcard_match_desc', '%', src_inputs]],
            [['cswm', '%', src_inputs], ['cswm_child', '%', src_inputs], ['cswm_desc', '%', src_inputs]]]

    exact = [[['exact_match', ' ', src_inputs], ['exact_match_child', ' ', src_inputs],
             ['exact_match_desc', ' ', src_inputs]],
             [['csem', ' ', src_inputs], ['csem_child', ' ', src_inputs],
             ['csem_desc', ' ', src_inputs]]]

    # queries that map source_codes to standard_codes
    std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', 'code_count', databases]
    standard = [['stand_terms', '', std_inputs], ['stand_terms_child', '', std_inputs],
                ['stand_terms_desc', '',  std_inputs]]

    # putt queries together in a single list
    queries = wild + exact, standard[:1]

    # PHENOTYPES
    sheets = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
              'Steroid-Induced Osteonecrosis_155', 'Systemic Lupus Erythematosus_1058']

    for sht in sheets:

        print('\n', '*' * 25, 'Processing Phenotype: {0}'.format(sht), '*' * 25, '\n')

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])

        # download data
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

        # group data types for processing
        data_groups = data.groupby(['source_domain', 'input_type', 'standard_vocabulary'])

        # loop over the data domains (e.g. drug, condition, measurement)
        for domain in [x for x in data_groups.groups if 'String' in x[1]]:
            grp_data = data_groups.get_group(domain)

            # run queries
            print('\n', '=' * 25, 'Running Queries: {0} domain'.format(domain[0]), '=' * 25, '\n')

            for query in queries[0][3:4]:
                print(query)

                # create spreadsheet name
                spreadsheet = '{0}_{1}_{2}'.format(sht.split('_')[0].upper(), domain[0].upper(), query[0][0])

                # run wildcard and exact match source and standard code queries
                src_queries(all_data, grp_data, url, databases[0], query + [queries[-1]], domain, spreadsheet)

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
