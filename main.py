####################
# main.py
# version 1.0.0
# Python 3.6.2
####################

# import auger
# import gspread_dataframe as gd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from scripts.big_query import *
from scripts.data_processor import GSProcessor


def domain_occurrence(data, url, databases):
    """Function takes a pandas dataframe and a string containing a url which redirects to a GitHubGist SQL query. The
    function formats a query and runs it against the input database(s). This function assumes that it will be passed
    two relational databases as an argument.

    Args:
        data: A pandas dataframe.
        url: A string containing a url which redirects to a GitHub Gist SQL query.
        databases: A list where the items are 'None' or a list of strings that are database names.

    Returns:
        A pandas data frame.
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

            sql_args = GSProcessor.code_format(group,
                                               ['code', 'standard_code', 'standard_vocabulary'], '', url.split('/')[-1])
            res = db.gbq_query(url, (db_, *sql_args))
            query_results.append(res.drop_duplicates())

        merged = pd.concat(query_results).drop_duplicates()

        # rename column
        merged.rename(columns={'drg_count': str(db_.split('_')[0]) + '_count'}, inplace=True)
        merged_res.append(merged)

    # merge results from the input databases together
    col_set1 = set(list(merged_res[0])).intersection(set(list(merged_res[1])))
    merged_comb = pd.merge(left=merged_res[0], right=merged_res[1], how='outer',
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

    print('There are {0} unique rows in the results data frame'.format(len(merged_results)))

    if input_source[4] is not None:
        results = domain_occurrence(merged_results, url[input_source[3]], input_source[4])
        return results
    else:
        return merged_results


def descriptive(data, plot_title, plot_x_axis, plot_y_axis):
    """Function takes a pandas data frame, and three strings that contain information about the data frame. The
    function then derives and prints descriptive statistics and histograms.

    Args:
        data: a pandas dataframe.
        plot_title: A string containing a title for the histogram.
        plot_x_axis: A string containing a x-axis label for the histogram.
        plot_y_axis: A string containing a y-axis label for the histogram.

    Return:
        None
    """

    # subset data to only include standard concepts and counts
    dat_plot = data.copy()
    plt_dat_chco = dat_plot[['standard_code', 'CHCO_count']].drop_duplicates()
    plt_dat_mimic = dat_plot[['standard_code', 'MIMICIII_count']].drop_duplicates()

    # get counts of drug occurrences by standard codes
    # CHCO
    print(plt_dat_chco[['standard_code']].describe())
    num_c = plt_dat_chco.loc[plt_dat_chco['CHCO_count'] > 0.0]['CHCO_count'].sum()
    denom_c = len(set(list(plt_dat_chco.loc[plt_dat_chco['CHCO_count'] > 0.0]['standard_code'])))
    print('CHCO: There are {0} Unique Standard Codes with Occurrence > 0'.format(denom_c))
    print('CHCO: The Average Occurrences per Standard Code is : {0} \n'.format(num_c / denom_c))

    # MIMIC
    print(plt_dat_mimic[['standard_code']].describe())
    num_m = plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_count'] > 0.0]['MIMICIII_count'].sum()
    denom_m = len(set(list(plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_count'] > 0.0]['standard_code'])))
    print('MIMICIII: There are {0} Unique Standard Codes with Occurrence > 0'.format(denom_m))
    print('MIMICIII: The Average Occurrences per Standard Code is : {0} \n'.format(num_m / denom_m))

    # generate plots
    f = plt.figure()
    with sns.axes_style("darkgrid"):
        f.add_subplot(1, 2, 1)
        sns.distplot(list(plt_dat_mimic['MIMICIII_count']), color="dodgerblue", label="MIMIC-III Count")
        plt.legend()
        plt.ylabel(plot_y_axis)
        f.add_subplot(1, 2, 2)
        sns.distplot(list(plt_dat_chco['CHCO_count']), color="red", label="CHCO Count")
        plt.legend()
    f.text(0.5, 0.98, plot_title, ha='center', va='center')
    f.text(0.5, 0.01, plot_x_axis, ha='center', va='center')
    plt.show()

    return None


def main():

    ########################
    # GBQ: query an existing table
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']
    gbq_db = GBQ('sandbox-tc', databases[0])

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # create list to store information on source queries
    source_queries = [['wildcard_match', '%', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSWM', '%', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSWM_child', '%', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSWM_desc', '%', ['str', 'source_id', 'source_domain', None, None]],
                      ['exact_match', '', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSEM', '', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSEM_child', '', ['str', 'source_id', 'source_domain', None, None]],
                      ['CSEM_desc', '', ['str', 'source_id', 'source_domain', None, None]]]

    # create list to store information on standard queries
    standard_queries = [['stand_terms', '', ['code', 'source_code', 'source_vocabulary', 'code_count',
                                             ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]],
                        ['stand_terms_children', '', ['code', 'source_code', 'source_vocabulary', 'code_count',
                                                      ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]],
                        ['stand_terms_desc', '', ['code', 'source_code', 'source_vocabulary', 'code_count',
                                                  ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]]]

    # list sheets to process
    sheets = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
              'Steroid-Induced Osteonecrosis_155', 'Systemic Lupus Erythematosus_1058']

    # loop over data sets
    for sht in sheets:
        print('\n' + '=' * len('Processing Phenotype: {0}'.format(sht)))
        print('Processing Phenotype: {0}'.format(sht))
        print('=' * len('Processing Phenotype: {0}'.format(sht)) + str('\n'))

        # load data from GoogleSheet
        all_data = GSProcessor(['Phenotype Definitions', sht])
        all_data.data_download()
        data = all_data.get_data().dropna(how='all', axis=1).dropna()
        data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

        data_groups = data.groupby(['source_domain', 'input_type'])
        # split this so that you are separating string-drug vs string-all others

        for x in [x for x in data_groups.groups if 'String' in x[1]]:
            temp_data = data_groups.get_group(x)
            temp_data_name = '-'.join(x)

            for src_query in [source_queries[0]]:
                print('=' * len('Running Source Query: {0}'.format(src_query[0])))
                print('Running Source Query: {0}'.format(src_query[0]))
                print('=' * len('Running Source Query: {0}'.format(src_query[0])))

                # run standard query
                source_results = regular_query(temp_data, src_query[2], src_query[1], gbq_db, url, src_query[0],
                                               databases[0])
                print(source_results[['source_string', 'source_code']].describe())
                print(source_results[['source_vocabulary']].describe())

                # write out source data to Google sheet
                spreadsheet_name = '{0}_{1}_{2}'.format(sht, temp_data_name
                                                        , src_query[0])
                tab_name = '_'.join(spreadsheet_name.split('_')[2:])

                # create new spreadsheet class
                all_data.create_spreadsheet(spreadsheet_name, 'callahantiff@gmail.com')
                temp_data = GSProcessor([spreadsheet_name, tab_name])
                temp_data.create_worksheet(tab_name)
                temp_data.set_worksheet(tab_name)
                temp_data.sheet_writer(temp_data, source_results)

                # process second half of queries -- getting standard codes
                for std_query in [standard_queries[0]]:
                    print('=' * len('Running Standard Query: {0}'.format(std_query[0])))
                    print('Running Standard Query: {0}'.format(std_query[0]))
                    print('=' * len('Running Standard Query: {0}'.format(std_query[0])))

                    data2 = source_results.copy()
                    data2 = data2.drop(['source_domain', 'source_name', 'input_type'], axis=1).drop_duplicates()
                    stand_results = regular_query(data2, std_query[2], std_query[1], gbq_db, url, std_query[0], databases[0])

                    # print descriptive stats
                    print(stand_results[['source_string', 'source_code']].describe())
                    print(stand_results[['source_vocabulary']].describe())
                    print(stand_results[['standard_code', 'standard_vocabulary']].describe())

                    # # generate histograms and output for occurrence counts
                    # descriptive(stand_results,
                    #             '{0}_{1}_{2}: Occurrence Counts'.format(sht, tab_name, std_query[0]),
                    #             'Drug Exposure Occurrence (Count)', 'Density')

                    # write out standard data to Google sheet
                    new_tab = '{0}_{1}'.format(tab_name, std_query[0])
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
