####################
# main.py
# version 1.0.0
# Python 3.6.2
####################

# import auger
# import gspread_dataframe as gd
import numpy as np
import pandas as pd
from requests.exceptions import ConnectionError
import time

from scripts.big_query import *
from scripts.data_processor import GSProcessor


def count_merger(databases, data, merged_results):
    """

    Args:
        databases: A list of Google BigQuery databases.
        data: A pandas dataframe containing the original data (before running SQL query to return occurrence counts)
        merged_results: A list of pandas dataframes to be merged.

    Returns:
        A pandas dataframe with merged and de-duplicated results.

    """
    col_set1 = set(list(merged_results[0])).intersection(set(list(merged_results[1])))
    merged_comb = pd.merge(left=merged_results[0], right=merged_results[1], how='outer',
                           left_on=list(col_set1), right_on=list(col_set1))

    # aggregate merged count results by standard_code
    db1_name = str(databases[0].split('_')[0]) + '_count'
    db2_name = str(databases[1].split('_')[0]) + '_count'
    merged_agg = merged_comb.fillna(0).groupby('standard_code', as_index=False).agg(
        {db1_name: lambda x: sum(set(x)), db2_name: lambda x: sum(set(x))})

    # combine results with full dataset
    col_set2 = set(list(data)).intersection(set(list(merged_agg)))
    merged_full = pd.merge(left=data, right=merged_agg, how='outer', left_on=list(col_set2), right_on=list(col_set2))

    # replace rows with no counts, stored as 'NaN' with zero
    merged_full[[db1_name, db2_name]] = merged_full[[db1_name, db2_name]].fillna(0)

    return merged_full.drop_duplicates()


def domain_occurrence(data, url=None, databases=None):
    """Function takes a pandas dataframe and a string containing a url which redirects to a GitHubGist SQL query. The
    function formats a query and runs it against the input database(s). This function assumes that it will be passed
    two relational databases as an argument.

    Args:
        data: A pandas dataframe.
        url: A string containing a url which redirects to a GitHub Gist SQL query.
        databases: A list where the items are 'None' or a list of strings that are database names.

    Returns:
        A pandas dataframe.
    """

    merged_res = []

    for db_ in databases:
        print('\n' + '#' * 50 + '\n' + 'Running query against {0}'.format(db_))
        db = GBQ('sandbox-tc', db_)

        # generate and run queries against SQL database
        query_results = []
        batch = data.groupby(np.arange(len(data)) // 15000)

        for name, group in batch:
            print('\n Processing chunk {0} of {1}'.format(name + 1, batch.ngroups))

            sql_args = GSProcessor.code_format(group, ['code'], '', url.split('/')[-1])
            res = db.gbq_query(url, (db_, *sql_args))
            query_results.append(res.drop_duplicates())

        merged = pd.concat(query_results).drop_duplicates()

        # rename column
        merged.rename(columns={'occ_count': str(db_.split('_')[0]) + '_count'}, inplace=True)
        merged_res.append(merged)

    # merge results from the input databases together
    return count_merger(databases, data, merged_res)


def regular_query(data, input_source, mod, gbq_db, url, query, gbq_database):
    """Function generates a SQL query and runs it against a Google BigQuery database. The returned results are
    then used in a second query designed to retrieve

    Args:
        data: A pandas dataframe.
        input_source: A list of strings that represent columns in a pandas dataframe. The function assumes that
        the list contains the following:
            (1) a string that indicates if the query uses input strings or codes
            (2) a string that indicates the name of the column that holds the source codes or strings
            (3) a string that indicates the name of the column that holds the source domain or source vocabulary
            (4) 'None' or a list of strings that are database names
        mod: A string that contains a character and is used to indicate whether or not a modifier should be used.
        gbq_db: A Google BigQuery object.
        url: A dictionary of urls; each url represents an SQL query.
        query: A string containing the name of an SQL query.
        gbq_database: A string containing the name of a Google Big Query database.

    Returns:
        A pandas dataframe.

    """

    query_results = []
    batch = data.groupby(np.arange(len(data)) // 15000)

    for name, group in batch:
        print('\n Processing chunk {0} of {1}'.format(name + 1, batch.ngroups))
        sql_args = GSProcessor.code_format(group, input_source, mod)
        res = gbq_db.gbq_query(url[query], (gbq_database, *sql_args))
        query_results.append(res.drop_duplicates())

    # get occurrence counts
    cont_results = pd.concat(query_results).drop_duplicates()

    # add back source_string (only for standard queries)
    if 'source_string' in cont_results:
        merged_results = cont_results.copy()
    else:
        merged_results = pd.merge(left=data[['source_code', 'source_string']].drop_duplicates(),
                                  right=cont_results, how='right', on='source_code').drop_duplicates()

    if input_source[0] != 'code' or len(merged_results) == 0:
        # we don't want to get occurrence counts for source string queries and when no query results are returned
        print('There are {0} unique rows in the results dataframe'.format(len(merged_results)))
        return merged_results

    else:
        return domain_occurrence(merged_results, url[input_source[4]], input_source[5])


def main():

    ########################
    # GBQ: query an existing table
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']
    gbq_db = GBQ('sandbox-tc', databases[0])

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
    std_inputs = ['code', 'source_code', 'source_vocabulary', 'source_domain', 'code_count',
                  ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]
    standard_queries = [['stand_terms', '', std_inputs],
                        ['stand_terms_child', '', std_inputs],
                        ['stand_terms_desc', '', std_inputs]]

    # list sheets to process
    sheets = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
              'Steroid-Induced Osteonecrosis_155', 'Systemic Lupus Erythematosus_1058']

    # loop over data sets
    for sht in sheets[:2]:
        print('\n' + '=' * len('Processing Phenotype: {0}'.format(sht)))
        print('Processing Phenotype: {0}'.format(sht))
        print('=' * len('Processing Phenotype: {0}'.format(sht)) + str('\n'))

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()
        data_groups = data.groupby(['source_domain', 'input_type', 'standard_vocabulary'])

        for x in [x for x in data_groups.groups if 'String' in x[1]]:
            grp_data = data_groups.get_group(x)
            grp_data_name = x[0]
            std_vocab = x[2]

            print('_' * len('Running Clinical Domain: {0}'.format(grp_data_name)))
            print('Running Source Query: {0} \n'.format(grp_data_name))

            for src_query in source_queries:
                print('=' * len('Running Source Query: {0}'.format(src_query[0])))
                print('Running Source Query: {0}'.format(src_query[0]))
                print('=' * len('Running Source Query: {0}'.format(src_query[0])))

                # run standard query
                source_results = regular_query(grp_data, src_query[2], src_query[1], gbq_db, url, src_query[0],
                                               databases[0])

                if len(source_results) != 0:
                    print(source_results[['source_string', 'source_code']].describe())
                    print(source_results[['source_vocabulary']].describe())

                    # write out source data to Google sheet
                    spreadsheet_name = '{0}_{1}_{2}'.format(sht.split('_')[0].upper(), grp_data_name.upper(), src_query[0])
                    tab_name = '_'.join(spreadsheet_name.split('_')[2:])

                    # create new spreadsheet class
                    all_data.create_spreadsheet(spreadsheet_name, 'callahantiff@gmail.com')
                    temp_data = GSProcessor([spreadsheet_name, tab_name])
                    temp_data.create_worksheet(tab_name)
                    temp_data.set_worksheet(tab_name)
                    temp_data.sheet_writer(temp_data, source_results[['source_string', 'source_code', 'source_name',
                                                                      'source_vocabulary']])

                    # pause to avoid time out
                    time.sleep(10)

                    # process second half of queries -- getting standard codes
                    for std_query in standard_queries:
                        print('=' * len('Running Standard Query: {0}'.format(std_query[0])))
                        print('Running Standard Query: {0}'.format(std_query[0]))
                        print('=' * len('Running Standard Query: {0}'.format(std_query[0])))

                        src_data = source_results.copy()
                        src_data = src_data.drop(['source_name', 'input_type'], axis=1).drop_duplicates()
                        stand_results = regular_query(src_data, std_query[2] + [std_vocab], std_query[1], gbq_db,
                                                      url, std_query[0], databases[0])

                        if len(stand_results) != 0:
                            # print descriptive stats
                            print(stand_results[['source_string', 'source_code']].describe())
                            print(stand_results[['source_vocabulary']].describe())
                            print(stand_results[['standard_code', 'standard_vocabulary']].describe())

                            # order columns
                            stand_results = stand_results[['source_string', 'source_code', 'source_name', 'source_vocabulary',
                                                           'standard_code', 'standard_name', 'standard_vocabulary',
                                                           'CHCO_count', 'MIMICIII_count']]

                            # # generate histograms and output for occurrence counts
                            # temp_data.descriptive(stand_results,
                            #                       '{0}_{1}_{2}: {3} Occurrence Counts'.format(sht, tab_name, std_query[0],
                            #                                                                   temp_data_name),
                            #                       'Drug Exposure Occurrence (Count)', 'Density')

                            # make sure connection is still live before writing out data
                            new_tab = '{0}_{1}'.format(tab_name, std_query[0])

                            try:
                                temp_data.create_worksheet(new_tab)
                                temp_data.set_worksheet(new_tab)
                                temp_data.sheet_writer(temp_data, stand_results)

                            except ConnectionError:
                                # write out standard data to Google sheet
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
