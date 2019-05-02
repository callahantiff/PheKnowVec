####################
# main.py
# Python 3.6.2
####################

import copy
import time as time

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

        # set data
        data_class.set_data(data)

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
            tab_name = '{0}_{1}'.format('_'.join(spreadsheet_name.split('_')[2:]), std_query[0])
            write_data(spreadsheet_name, tab_name, std_results)

    return None


def write_data(spreadsheet_name, tab_name, results):
    """Writes a pandas dataframe to a specific tab in a GoogleSheet.

    Args:
        spreadsheet_name: A string containing the name of a GoogleSheet spreadsheet.
        tab_name:A string containing the name of a tab within a GoogleSheet spreadsheet.
        results: A pandas dataframe.

    Returns:
        None.

    """

    # pause for api
    time.sleep(10)

    # write data to GoogleSheet
    temp_data = GSProcessor([spreadsheet_name, tab_name])
    temp_data.create_worksheet(tab_name)
    temp_data.set_worksheet(tab_name)
    temp_data.sheet_writer(temp_data, results)

    return None


def src_queries(data_class, src_type, data, url, database, queries, standard_vocab, phenotype):
    """Runs a list of queries against a Google BigQuery database designed to map a set of source strings or codes to
    OMOP concept_codes. The results are returned as a pandas dataframe. These results are then passed to a second
    function which maps the OMOP source concept_codes to concept_ids in a standard OMOP vocabulary. Finally,
    the results are written to a Google sheet via the GoogleDrive API.

    Args:
        data_class: A GoogleSheets class instance.
        src_type: A string containing the name of the initial query.
        data: A pandas dataframe.
        url: A dictionary where keys are query names and values are url strings.
        database: A string that points to a Google BigQuery database.
        queries: A list of query lists. The function assumes that this list contains three lists: 1) wildcard match,
        exact match, and standard.
        standard_vocab: A string that contains the name of a OMOP standard vocabulary to map source codes to.
        phenotype: A string that contains the name of the phenotype.

    Returns:
        None.
    """
    # define queries and arguments
    if 'wild' in src_type:
        src_queries_ = queries[0]
    else:
        src_queries_ = queries[1]

    # run query
    for query in src_queries_:
        print('\n', '=' * 25, 'Running Source Query: {0}'.format(query[0]), '=' * 25, '\n')

        # set data
        data_class.set_data(data)

        src_results = data_class.regular_query(query, 'sandbox-tc', url, database)

        # make sure the query returned valid results
        if len(src_results) != 0:

            # order columns and rows
            source_res_cpy = src_results.copy()
            source_res_cpy = source_res_cpy[['source_string', 'source_code', 'source_name', 'source_vocabulary']]
            source_res_cpy = source_res_cpy.sort_values(by=['source_string', 'source_code', 'source_vocabulary'],
                                                        ascending=True)
            # create spreadsheet and write out results
            sheet_name = '{0}_{1}_{2}'.format(phenotype.split('_')[0].upper(), standard_vocab[0].upper(), query[0])
            tab_name = '{0}'.format(query[0])
            temp_data = GSProcessor([sheet_name, tab_name])
            temp_data.create_spreadsheet(sheet_name, 'callahantiff@gmail.com')

            write_data(sheet_name, tab_name, source_res_cpy)

            # run standard queries and write results
            standard_queries(data_class, src_results, queries[-1], url, database, standard_vocab, sheet_name)

    return None


def main():

    ########################
    # list databases
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # QUERY ARGUMENTS
    # initial queries
    initial_queries = [['wildcard_match', '%', ['str', 'source_id', 'source_domain']],
                       ['exact_match', '', ['str', 'source_id', 'source_domain']]]

    # source queries which are run on the results from initial queries
    src_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', None]
    wild = [['wildcard_match_child', '', src_inputs], ['wildcard_match_desc', '', src_inputs],
            ['cswm', '', src_inputs], ['cswm_child', '', src_inputs], ['cswm_desc', '', src_inputs]]

    exact = [['exact_match_child', '', src_inputs], ['exact_match_desc', '', src_inputs],
             ['csem', '', src_inputs], ['csem_child', '', src_inputs], ['csem_desc', '', src_inputs]]

    # standard queries which are run on the results from source_queries
    std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', 'code_count', databases]
    standard = [['stand_terms', '', std_inputs], ['stand_terms_child', '', std_inputs],
                ['stand_terms_desc', '',  std_inputs]]

    # PHENOTYPES
    sheets = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
              'Steroid-Induced Osteonecrosis_155', 'Systemic Lupus Erythematosus_1058']

    for sht in sheets:
        print('\n', '*' * 25, 'Processing Phenotype: {0}'.format(sht), '*' * 25, '\n')

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

        # group data types for processing
        data_groups = data.groupby(['source_domain', 'input_type', 'standard_vocabulary'])

        # loop over the data domains (e.g. drug, condition, measurement)
        for domain in [x for x in data_groups.groups if 'String' in x[1]]:
            grp_data = data_groups.get_group(domain)

            # run queries
            for query in initial_queries:
                print('\n', '=' * 25, 'Running Query: {0} on {1} domain'.format(query[0], domain[0]), '=' * 25, '\n')

                # reset instance data
                all_data.set_data(grp_data)

                # run query
                source_results = all_data.regular_query(query, 'sandbox-tc', url, databases[0])

                # order columns and rows
                source_res_cpy = source_results.copy()
                source_res_cpy = source_res_cpy[['source_string', 'source_code', 'source_name', 'source_vocabulary']]
                source_res_cpy = source_res_cpy.sort_values(by=['source_string', 'source_code', 'source_vocabulary'],
                                                            ascending=True)

                # create spreadsheet and write out results
                sheet_name = '{0}_{1}_{2}'.format(sht.split('_')[0].upper(), domain[0].upper(), query[0])
                all_data.create_spreadsheet(sheet_name, 'callahantiff@gmail.com')
                write_data(sheet_name, '_'.join(sheet_name.split('_')[2:]), source_res_cpy)

                # get standard terms for initial query
                standard_queries(all_data, source_results, standard, url, databases[0], domain, sheet_name)

                # run wildcard and exact match source and standard code queries
                queries = [wild[2:], exact, standard[-1:]]
                src_queries(all_data, query[0], source_results, url, databases[0], queries, domain, sht)

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
