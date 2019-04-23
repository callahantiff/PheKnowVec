####################
# main.py
# version 1.0.0
# Python 3.6.2
####################

# import auger
import gspread_dataframe as gd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from scripts.big_query import *
from scripts.data_processor import GSProcessor


def domain_occurrence(data, url, databases):
    """Function takes a pandas dataframe and a string containing a url which redirects to a GitHub Gist SQL
    query. The function generates a query and runs it against the CHCO de-identified and MIMIC-III OMOP database.

    Args:
        data: A pandas dataframe.
        url: A string containing a url which redirects to a GitHub Gist SQL query.
        databases: A list where the items are 'None' or a list of strings that are database names.

    Returns:
        A pandas data frame.
    """

    merged_results = []

    for db_ in databases:
        print('\n' + '#' * 50 + '\n' + 'Running query against {0}'.format(db_))
        db = GBQ('sandbox-tc', db_)

        # query data
        query_results = []
        batch = data.groupby(np.arange(len(data)) // 15000)

        for name, group in batch:
            print('\n Processing chunk {0} of {1}'.format(name + 1, batch.ngroups))

            sql_args = GSProcessor.code_format(group, ['code', 'standard_code', 'standard_vocabulary'], '')
            res = db.gbq_query(url, (db_, *sql_args))
            query_results.append(res.drop_duplicates())

        merged = pd.concat(query_results).drop_duplicates()

        # rename column
        merged.rename(columns={'drg_count': str(db_.split('_')[0]) + '_drg_count'}, inplace=True)
        merged_results.append(merged)

    # merge counts results together
    merge_col1 = set(list(merged_results[0])).intersection(set(list(merged_results[1])))
    merged_comb = pd.merge(left=merged_results[0], right=merged_results[1], how='outer',
                           left_on=list(merge_col1), right_on=list(merge_col1))

    # group merged count results by source_code
    if 'source_string' in list(data):
        merged_comb_agg1 = merged_comb.fillna(0).groupby(['source_code', 'standard_code'], as_index=False).agg({
            'CHCO_drg_count': lambda x: sum(set(x)), 'MIMICIII_drg_count': lambda x: sum(set(x))})
    else:
        merged_comb_agg1 = merged_comb.fillna(0).groupby('standard_code', as_index=False).agg(
            {'CHCO_drg_count': lambda x: sum(set(x)), 'MIMICIII_drg_count': lambda x: sum(set(x))})

    # combine results with full dataset
    merge_col2 = set(list(data)).intersection(set(list(merged_comb_agg1)))
    merged_full = pd.merge(left=data, right=merged_comb_agg1, how='outer',
                           left_on=list(merge_col2), right_on=list(merge_col2))

    # replace rows with no counts with zero
    merged_full[['CHCO_drg_count', 'MIMICIII_drg_count']] = merged_full[['CHCO_drg_count',
                                                                         'MIMICIII_drg_count']].fillna(0)

    return merged_full.drop_duplicates()


def reg_query(data, input_source, mod, gbq_db, url, query, gbq_database):
    """Function generates a SQL query and runs it against a Google Big Query database. The returned results are
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
        gbq_db: A Google Big Query object.
        url: A dictionary of urls; each url represents an SQL query.
        query: A string containing the name of an SQL query.
        gbq_database: A string containing the name of a Google Big Query database.

    Returns:

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
    print('There are {0} unique rows in the results data frame'.format(len(cont_results)))

    if input_source[4] is not None:
        results = domain_occurrence(cont_results, url[input_source[3]], input_source[4])
        return results
    else:
        return cont_results


def main():
    # # instantiate class and read in phenotypes
    # all_data = GSProcessor(['Phenotype Definitions', 'ADHD_test'])
    # all_data.data_download()
    # data = all_data.get_data().dropna(how='all', axis=1).dropna()
    # data = data.drop(['cohort', 'criteria', 'phenotype_criteria'], axis=1).drop_duplicates()

    ########################
    # GBQ: query an existing table
    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']
    gbq_db = GBQ('sandbox-tc', databases[0])

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}

    # load data
    sht = 'ADHD_test'
    all_data = GSProcessor(['Phenotype Definitions', sht])
    all_data.data_download()
    data = all_data.get_data().dropna(how='all', axis=1).dropna()
    data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'], axis=1).drop_duplicates()

    for dat in [['automated', '%', ['str', 'source_id', 'source_domain', 'SRC_DRG_COUNT', None]],
                ['exact_match', '', ['str', 'source_id', 'source_domain', 'SRC_DRG_COUNT', None]],
                ['CSEM_OMOP_KE', '', ['str', 'source_id', 'source_domain', 'SRC_DRG_COUNT', None]],
                ['CSEM_OMOP_KE_Children', '', ['str', 'source_id', 'source_domain', 'SRC_DRG_COUNT', None]],
                ['CSEM_OMOP_KE_Desc', '', ['str', 'source_id', 'source_domain', 'SRC_DRG_COUNT', None]]]:

        source_results = reg_query(data, dat[2], dat[1], gbq_db, url, dat[0], databases[0])
        print(source_results[['source_string', 'source_code', 'source_vocabulary']].describe())

        # write out source data
        sht_new = '{0}_{1}'.format(sht, dat[0])
        all_data.create_worksheet(sht_new)
        all_data.set_worksheet(sht_new)
        gd.set_with_dataframe(all_data.get_worksheet(), source_results)

        # process second half of queries -- getting standard codes
        for dat2 in [['stand_terms', '', ['code', 'source_code', 'source_vocabulary', 'CODE_DRG_COUNT',
                                          ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]],
                     ['stand_terms_children', '', ['code', 'source_code', 'source_vocabulary', 'CODE_DRG_COUNT',
                                                   ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]],
                     ['stand_terms_desc', '', ['code', 'source_code', 'source_vocabulary', 'CODE_DRG_COUNT',
                                               ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']]]]:
            data2 = source_results.copy()
            data2 = data2.drop(['source_string', 'source_domain', 'source_name', 'input_type'],
                               axis=1).drop_duplicates()

            stand_results = reg_query(data2, dat2[2], dat2[1], gbq_db, url, dat2[0], databases[0])

            # print descriptive stats
            print(stand_results[['source_code', 'source_vocabulary']].describe())
            print(stand_results[['standard_code', 'standard_vocabulary']].describe())

            # subset data to only include standard concepts and counts
            dat_plot = stand_results.copy()
            plt_dat_chco = dat_plot[['standard_code', 'CHCO_drg_count']].drop_duplicates()
            plt_dat_mimic = dat_plot[['standard_code', 'MIMICIII_drg_count']].drop_duplicates()

            # get counts of drug occurrences by standard codes
            print(plt_dat_chco[['standard_code']].describe())
            num_c = plt_dat_chco.loc[plt_dat_chco['CHCO_drg_count'] > 0.0]['CHCO_drg_count'].sum()
            denom_c = len(set(list(plt_dat_chco.loc[plt_dat_chco['CHCO_drg_count'] > 0.0]['standard_code'])))
            print(denom_c)
            print(num_c / denom_c)

            print(plt_dat_mimic[['standard_code']].describe())
            num_m = plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_drg_count'] > 0.0]['MIMICIII_drg_count'].sum()
            denom_m = len(set(list(plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_drg_count'] > 0.0]['standard_code'])))
            print(denom_m)
            print(num_m / denom_m)

            f = plt.figure()
            with sns.axes_style("darkgrid"):
                f.add_subplot(1, 2, 1)
                sns.distplot(list(plt_dat_mimic['MIMICIII_drg_count']), color="dodgerblue", label="MIMIC-III Count")
                plt.legend()
                plt.ylabel('Density')
                f.add_subplot(1, 2, 2)
                sns.distplot(list(plt_dat_chco['CHCO_drg_count']), color="red", label="CHCO Count")
                plt.legend()
            f.text(0.5, 0.98, '{0}_{1}_{2}: Drug Occurrence Counts'.format(sht, dat[0], dat2[0]), ha='center',
                   va='center')
            f.text(0.5, 0.01, 'Drug Exposure Occurrence (Count)', ha='center', va='center')
            plt.show()

            # write out standard data
            sht_new = '{0}_{1}_{2}'.format(sht, dat[0], dat2[0])
            all_data.create_worksheet(sht_new)
            all_data.set_worksheet(sht_new)
            gd.set_with_dataframe(all_data.get_worksheet(), stand_results)

    # get source_code mappings
    # code_url = url[0].split(';')[-1]
    # sql_code_args = all_data.code_format(data)
    # code_res = db.gbq_query(code_url, sql_code_args)

    # sht_list = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
    #             'Sickle Cell Disease_615', 'Sleep Apnea_240', 'Steroid-Induced Osteonecrosis_155',
    #             'Systemic Lupus Erythematosus_1058']
    # sht_list = ['ADHD_179', 'Appendicitis_236', 'Crohns Disease_77', 'Hypothyroidism_14', 'Peanut Allergy_609',
    #               'Sickle Cell Disease_615', 'Sleep Apnea_240', 'Steroid-Induced Osteonecrosis_155',
    #               'Systemic Lupus Erythematosus_1058']
    #
    # for db_name in ('CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019'):
    #     print('#' * 100 + '\n')
    #     print('Processing Database: {0}'.format(db_name))
    #
    #     db = GBQ('sandbox-tc', db_name)
    #     url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists.txt', 'r').read().split('\n')}
    #
    #     for sheet in sht_list:
    #         if '_' in str(sheet):
    #             # sht = str(sheet).split("'")[1]
    #             sht = sheet
    #
    #             try:
    #                 print('#' * 50 + '\n')
    #                 print('Running Google Sheet: {0}, Tab: {1}'.format('Phenotype Definitions', sht))
    #
    #                 all_data = GSProcessor(['Phenotype Definitions', sht])
    #                 all_data.data_download()
    #                 data = all_data.get_data().dropna(how='all', axis=1).dropna()
    #                 data = data.drop(['cohort', 'criteria', 'phenotype_criteria', 'phenotype'],
    #                 axis=1).drop_duplicates()
    #
    #                 print(len(data.groupby('input_type').get_group('Code')))
    #
    #                 # get source_string to source_code mappings
    #                 if 'Drug' in data.groupby('source_domain').groups.keys():
    #                     print('Running Synonyms Query against Database: {0}'.format(db_name))
    #                     sql_string_syn = all_data.string_format(data.groupby('source_domain').get_group('Drug'))
    #                     syn_res = db.gbq_query(url['source_string_syn'], sql_string_syn)
    #                     data = pd.concat([data, syn_res], axis=0, sort=True).drop_duplicates()
    #
    #                 print('Running Query against Database: {0}'.format(db_name))
    #                 string_url = url['source_string']
    #                 sql_string_args = all_data.string_format(data)
    #                 string_res = db.gbq_query(string_url, sql_string_args)
    #
    #                 # merge results
    #                 print('Merging Results')
    #                 merge_cols = ['source_code', 'source_domain', 'source_string', 'source_domain']
    #                 keep_cols = ['source_vocabulary', 'source_code', 'source_name']
    #                 merged_res = all_data.result_merger(data, string_res, merge_cols, keep_cols)
    #
    #                 # export merged data to Google Sheet
    #                 print('\n Writing {0} results to Google Sheet'.format(len(merged_res)))
    #                 sht_new = str(sht) + '_' + str(db_name).split('_')[0] + '_SrcStr2Code'
    #                 all_data.create_worksheet(sht_new)
    #                 all_data.set_worksheet(sht_new)
    #                 gd.set_with_dataframe(all_data.get_worksheet(), merged_res)
    #
    #             except KeyError:
    #                 print("Skipping worksheet as it contains no Strings to process")
    #                 pass
    # for qury in ['stand_terms', 'stand_terms_children', 'stand_terms_desc']:
    #     string_url = url[qury]
    #     df_results1 = []
    #     batch_data1 = data.groupby(np.arange(len(data)) // 15000)
    #
    #     for name, group in batch_data1:
    #         sql_code_args = code_format(group)
    #         code_res = gbq_query(string_url, (db_[0], *sql_code_args)).drop_duplicates()
    #         df_results1.append(code_res.drop_duplicates())
    #
    #     string_res = pd.concat(df_results1).drop_duplicates()
    #     print(string_res[['source_code']].describe())
    #     print(string_res[['source_vocabulary']].describe())
    #     print(string_res[['standard_code']].describe())
    #     print(string_res[['standard_vocabulary']].describe())
    #     # print(string_res[['source_string', 'source_vocabulary']].describe())
    #
    #     # get counts of drg_occurrence
    #     for x in db:
    #         df_results = []
    #         batch_data = string_res.groupby(np.arange(len(string_res)) // 15000)
    #
    #         for name, group in batch_data:
    #             sql_drg_args = string_format2(group)
    #
    #             if q in []
    #             drg_res = gbq_query(url["CODE_DRG_COUNT"], (db_[0], *sql_drg_args)).drop_duplicates()
    #             print(len(drg_res.drop_duplicates()))
    #             df_results.append(drg_res.drop_duplicates())
    #
    #         string_res2 = pd.concat(df_results).drop_duplicates()
    #
    #     # export merged data to Google Sheet
    #     # print('\n Writing {0} results to Google Sheet'.format(len(merged_res)))
    #     sht_new = 'ADHD_test_' + str(dat) + "_" + str(qury)
    #     all_data.create_worksheet(sht_new)
    #     all_data.set_worksheet(sht_new)
    #     gd.set_with_dataframe(all_data.get_worksheet(), string_res)

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
