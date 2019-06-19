####################
# main.py
# Python 3.6.2
####################

import copy
import pandas as pd
import time

from scripts.data_processor import GSProcessor
from scripts.big_query import *


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
        If spreadsheet_name is empty and the query returned results then a Pandas data frame of standard code
        results is returned, otherwise None is returned and the results are written to a Google Sheet.
    """

    # process second half of queries -- getting standard codes
    std_query_res = []
    for std_query in queries:
        print('\n', '=' * 25, 'Running Standard Query: {0}'.format(std_query[0]), '=' * 25, '\n')
        time.sleep(10)

        # set instance data
        data_class.set_data(data)

        # copy query list before inserting a new list into it
        std_query_mod = copy.deepcopy(std_query)
        std_query_mod[2].insert(len(std_query_mod[2]), list(standard_vocab)[2:])

        # run query -- to be accurate when mapping source codes to standard codes, we run queries by vocabulary
        stand_res = []

        # group by source vocabularies for processing
        data_groups = data.groupby(['source_vocabulary'])
        x = 0

        for vocab in data_groups.groups:
            x += 1
            print('\n Processing vocabulary chunks: {0}/{1}'.format(x, data_groups.ngroups))

            # set instance data + run query
            data_class.set_data(data_groups.get_group(vocab))
            stand_res.append(data_class.regular_query(std_query_mod, 'sandbox-tc', url, database))

        # combine results
        std_results = pd.concat(stand_res, sort=True).drop_duplicates()

        if len(std_results) != 0:

            if len(spreadsheet_name) == 2:
                # write out results
                tab_name = '{0}_{1}'.format(spreadsheet_name[1], std_query[0])
                data_class.authorize_client()
                data_class.write_data(spreadsheet_name[0], tab_name, std_results)

                return None

            else:
                if spreadsheet_name[0] == '':
                    code_set_name = std_query[0]
                else:
                    code_set_name = spreadsheet_name[0] + '_' + std_query[0]

                std_results['standard_code_set'] = code_set_name
                std_query_res.append(std_results)

    # verify that the query returned results  -- only returning if more than 0 rows of data
    if len(std_query_res) > 0:
        return pd.concat(std_query_res, sort=True).drop_duplicates()


def src_queries(data_class, data, url, database, queries, standard_vocab, spreadsheet, write_opt):
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
        spreadsheet: A string that contains the name of the spreadsheet to write out results to.
        write_opt: A string that contains information on whether to write data to file or not.

    Returns:
        If write_opt is empty then a Pandas data frame of source and standard code results is returned,
        otherwise None is returned and the results are written to a Google Sheet.
    """
    src_std_results = []
    for query in queries[:-1]:
        print('\n', '=' * 25, 'Running Source Query: {query_name}'.format(query_name=query[0]), '=' * 25, '\n')

        # set data
        data_class.set_data(data)

        # run query
        src_results = data_class.regular_query(query, 'sandbox-tc', url, database)

        # make sure the query returned valid results
        if len(src_results) != 0:

            # add back source strings that returned no results
            input_str = set([x.replace('"', '').strip() for x in list(data_class.data['source_id'].drop_duplicates())])
            input_str_diff = input_str.difference(set(list(src_results['source_string'])))
            str_domains = pd.DataFrame(input_str_diff, columns=['source_string'])
            src_results_merge = pd.concat([src_results, str_domains], axis=0, ignore_index=True, sort=True)

            # order columns and rows
            source_res_cpy = src_results_merge.copy().fillna('')
            source_res_cpy = source_res_cpy[['source_string', 'source_code', 'source_name', 'source_vocabulary']]
            source_res_cpy = source_res_cpy.sort_values(by=['source_string', 'source_code', 'source_vocabulary'],
                                                        ascending=True)

            # verify if writing data to file or back to GBQ
            if write_opt != 'file':
                # check data dimensions to ensure we can write data
                data_set_size = len(source_res_cpy) * len(list(source_res_cpy))

                # sleep system and re-authorize API client
                time.sleep(30)
                data_class.authorize_client()

                # try running  code again
                if spreadsheet in {sheet.title: sheet.id for sheet in data_class.client.openall()}.keys():
                    if data_class.count_spreadsheet_cells(spreadsheet) + data_set_size < 5000000:
                        # set tab and write out results
                        tab_name = '{0}'.format(query[0])
                        data_class.authorize_client()
                        data_class.write_data(spreadsheet, tab_name, source_res_cpy)
                    else:
                        # write out results to a new spreadsheet named after the query
                        new_sheet = '{0}_{1}'.format('_'.join(spreadsheet.split('_')[0:2]), query[0])
                        tab_name = '{0}'.format('_'.join(spreadsheet.split('_')[2:]))
                        data_class.authorize_client()
                        data_class.write_data(new_sheet, tab_name, source_res_cpy)
                else:
                    # when spreadsheet does not yet exist -- write out results
                    tab_name = '{0}'.format(query[0])
                    data_class.authorize_client()
                    data_class.write_data(spreadsheet, tab_name, source_res_cpy)

                # run standard queries and write results
                src_spreadsheet = [spreadsheet, tab_name]
                standard_queries(data_class, src_results, queries[-1], url, database, standard_vocab, src_spreadsheet)

            else:
                # sleep system and re-authorize API client
                time.sleep(30)
                data_class.authorize_client()

                # run standard queries and write results
                st_data = standard_queries(data_class, src_results, queries[-1], url, database, standard_vocab,
                                           [query[0]])
                st_data['source_code_set'] = query[0]

                # append results
                src_std_results.append(st_data)

            # sleep system and re-authorize API client
            time.sleep(10)
            data_class.authorize_client()

    # only return results if there is data
    if len(src_std_results) > 0:
        return pd.concat(src_std_results, sort=True).drop_duplicates()

    else:
        return None


def source_code_populator(queries, std_results):
    """takes a list of queries and a pandas data frame of results and replicates the data frame n times, where n is
    the number of source queries. This is only performed when a source code is provided as the input. We do this
    because we assume that we  have the final set of source codes, which differs from when a string is given as input
    and we have to explore all potential mappings.

    Args:
        queries: a list of lists, where each list holds a set of lists and each of those sub-lists contains query
        information.
        std_results: a pandas data frame of query results.

    Returns:
         An updated pandas data frame.
    """
    query_res_src = []
    for src in [x[0] for y in queries[0] for x in y]:

        # copy data frame
        query_res_cpy = std_results.copy()

        # add source code label
        query_res_cpy['source_code_set'] = src
        std_code_set = query_res_cpy['standard_code_set'].apply(lambda x: src + '_' + x)
        query_res_cpy['standard_code_set'] = std_code_set

        query_res_src.append(query_res_cpy)

    concat_query_res = pd.concat(query_res_src, sort=True).drop_duplicates()

    if len(concat_query_res) == len(std_results) * len([x[0] for y in queries[0] for x in y]):
        return concat_query_res
    else:
        raise ValueError('Error - the number of replicated rows is incorrect')


def main():
    ########################
    # list databases
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # QUERY ARGUMENTS
    # queries that map input strings to source_codes
    src_inputs1 = ['str', 'source_id', 'source_domain']
    src_inputs2 = ['str_syn', 'source_id', 'source_domain']
    wild = [[['fuzzy_none_self', '%', src_inputs1], ['fuzzy_none_child', '%', src_inputs1],
             ['fuzzy_none_desc', '%', src_inputs1]], [['fuzzy_syn_self', '%', src_inputs2],
                                                      ['fuzzy_syn_child', '%', src_inputs2],
                                                      ['fuzzy_syn_desc', '%', src_inputs2]]]

    exact = [[['exact_none_self', ' ', src_inputs1], ['exact_none_child', ' ', src_inputs1],
              ['exact_none_desc', ' ', src_inputs1]], [['exact_syn_self', ' ', src_inputs2],
                                                       ['exact_syn_child', ' ', src_inputs2],
                                                       ['exact_syn_desc', ' ', src_inputs2]]]

    # queries that map source_codes to standard_codes
    # std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', 'code_count', databases]
    std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', '', databases]
    standard = [['stand_none_self', '', std_inputs], ['stand_none_child', '', std_inputs],
                ['stand_none_desc', '', std_inputs]]

    # put queries together in a single list
    queries = wild + exact, standard

    # PHENOTYPES
    sheets = ['ADHD_179', 'SickleCellDisease_615', 'SleepApnea_240', 'Appendicitis_236', 'CrohnsDisease_77',
              'Hypothyroidism_14', 'PeanutAllergy_609', 'SteroidInducedOsteonecrosis_155',
              'SystemicLupusErythematosus_1058']

    for sht in sheets[4:5]:
        print(sht)
        print('\n', '*' * 25, 'Processing Phenotype: {phenotype}'.format(phenotype=sht), '*' * 25, '\n')

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])
        all_data.set_worksheet(sht)

        # download data
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        # data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

        # group data types for processing
        data_groups = data.groupby(['source_domain', 'input_type', 'standard_vocabulary'])

        # create variable to store data
        domain_results = []

        # loop over the data domains (e.g. drug, condition, measurement)
        for domain in data_groups.groups:
            print(domain)

            if 'String' in domain[1]:
                # run queries
                print('\n', '=' * 25, 'Running Queries: {id} domain'.format(id=domain[0]), '=' * 25, '\n')

                # store results
                query_res = []
                grp_data = data_groups.get_group(domain)
                process_data = grp_data.copy()

                for query in queries[0]:
                    all_data.authorize_client()

                    # create spreadsheet name
                    spreadsheet = '{0}_{1}_{2}'.format(sht.split('_')[0].upper(), domain[0].upper(), query[0][0])

                    # run wildcard and exact match source and standard code queries
                    res = src_queries(all_data, process_data, url, databases[0], query + [queries[-1]], domain,
                                      spreadsheet, 'file')

                    if res is not None:
                        # fix formatting of source_string
                        res['source_string'] = ['"' + str(x) + '"' for x in list(res['source_string'])]

                        # re-order columns
                        res = res[['source_string', 'source_code', 'source_concept_id', 'source_name', 'source_domain',
                                   'source_vocabulary', 'source_code_set', 'standard_code', 'standard_concept_id',
                                   'standard_name', 'standard_domain', 'standard_vocabulary', 'standard_code_set']]

                        query_res.append(res)
                        time.sleep(60)
            else:
                # run queries
                print('\n', '=' * 25, 'Running Queries: {id} domain'.format(id=domain[0]), '=' * 25, '\n')

                # store results
                grp_data = data_groups.get_group(domain)

                # rename column
                process_data = grp_data.copy()
                process_data['source_code'] = list(process_data['source_id'])
                process_data['source_string'] = None

                # run queries
                print('\n', '=' * 25, 'Running Queries: {id} domain'.format(id=domain[0]), '=' * 25, '\n')

                # run standard code queries
                query_res = standard_queries(all_data, process_data, standard, url, databases[0], domain, [''])
                query_res['source_domain'] = domain[0]
                query_res['source_string'] = query_res['source_code']
                query_res['source_code_set'] = 'exact_none_self'

                # add source codes
                updated_query_res = source_code_populator(queries, query_res)

                # re-order columns
                query_res = updated_query_res[['source_string', 'source_code', 'source_concept_id', 'source_name',
                                               'source_domain', 'source_vocabulary', 'source_code_set',
                                               'standard_code', 'standard_concept_id', 'standard_name',
                                               'standard_domain', 'standard_vocabulary', 'standard_code_set']]
                time.sleep(60)

            # append domain data to list of domain data
            if len(query_res) > 0:
                if 'String' in domain[1]:
                    domain_results.append(pd.concat(query_res, sort=True).drop_duplicates())
                else:
                    domain_results.append(query_res.drop_duplicates())

        # combine results
        concat_domain_results = pd.concat(domain_results, sort=True).drop_duplicates()

        # add columns from original data set
        merged_domain_results = pd.merge(left=data[['cohort', 'criteria', 'phenotype_definition_number',
                                                    'phenotype_definition_label', 'input_type',
                                                    'source_id']],
                                         right=concat_domain_results, how='outer',
                                         left_on='source_id', right_on='source_string')

        # merged_domain_results.to_csv(r'export_dataframe.csv', index=None, header=True)

        # write data to CHCO + MIMIC databases
        for db in databases:
            # GBQ: create a new table -- after verifying mappings
            db_conn = GBQ('sandbox-tc', db)

            # create a new table + write data to database
            table_name = sht.split('_')[0].upper() + '_COHORT_VARIABLES'
            db_conn.create_table(table_name, merged_domain_results)

        time.sleep(90)

#
# for index, row in merged_domain_results.iterrows():
#     if row['standard_code_set'] == 'stand_none_self' or row['standard_code_set'] == 'stand_none_child' or \
#             row['standard_code_set'] == 'stand_none_desc':
#         print(row)

# merge_test = pd.merge(left=data[['cohort', 'criteria', 'phenotype_definition_number',
#                                  'phenotype_definition_label', 'input_type', 'source_id']],
#                                          right=res, how='outer',
#                                          left_on='source_id', right_on='source_string')
#
# merged_domain_results.to_csv(r'export_dataframe.csv', index=None, header=True)

# if __name__ == '__main__':
#     # with auger.magic([GSProcessor], verbose=True):  # this is the new line and invokes Auger
#     main()
