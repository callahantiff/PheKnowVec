##########################
# cohort_creator.py
# Python 3.6.2
##########################

import requests
import pandas as pd

from scripts.big_query import GBQ


def queries_gbq(phenotype, queries, code_sets, db):
    """Executes a list of queries against a database for each code set.

    Args:
        phenotype: A string naming the phenotype.
        queries: A url pointing to a SQL query.
        code_sets: A list of strings, where each string is a code set.
        db: A string naming a Google Big Query database.

    Returns:
        A Pandas data frame of query results.

    """

    phenotype_results = []

    # initiate GBQ class
    gbq_query = GBQ('sandbox-tc', db)

    for query in queries:
        print('\n')

        for grp_set in code_sets:
            print('Running Query: {query} - Code Set: {codeset}'.format(query=query.split('/')[-1], codeset=grp_set))

            # format query
            group = '"' + str(grp_set) + '"'
            formatted_query = requests.get(query, allow_redirects=True).text.format(database=db, code_set_group=group)

            # run query
            phenotype_results.append(gbq_query.gbq_query(formatted_query))

    # concatenate results
    merged_results = pd.concat(phenotype_results, sort=True).drop_duplicates()

    # write data to a file
    table = str(db) + '_' + phenotype.split('_')[0] + '_COHORT.csv'

    print('-' * len('Writing Results - File Path: '))
    print('Writing Results - File Path: {data}'.format(data=r'temp_results/cohorts/' + str(table)))
    print('-' * len('Writing Results - File Path: '))

    merged_results.to_csv(r'temp_results/cohorts/' + str(table), index=None, header=True, encoding='utf-8')

    return merged_results


def evaluates_performance(y_actual, y_predicted):
    """Calculates the true positive (tp), false positive (fp), false negative (fn), false negative rate (fnr),
    and false positive rate (fpr). The total count of gold standard patients and predicted patients is also
    calculated and returned.

    Args:
        y_actual: A list of people identifiers; gold standard.
        y_predicted: A list of people identifiers; predicted.

    Returns:
        A nested list of performance metrics where the first item is a string of formatted results and the second
        item is the string represented as a list. For example:

        (
        'ActTot:9726; PredTot:9726; TP:9726; FN:0; FNR:0.0; FP:0; FPR:0.0',
        [9726, 9726, 9726, 0, 0.0, 0, 0.0]
        )

    """
    tp = set(y_actual).intersection(set(y_predicted))

    # number of patients inappropriately included in gs (in y_pred and not y_actual)
    fp = set(y_predicted).difference(set(y_actual))

    # number of patients missing gs
    fn = set(y_actual).difference(set(y_predicted))

    # calculate FNR/FPR
    fnr = len(fn) / float(len(tp))
    fpr = len(fp) / float(len(tp))

    # format results
    performance_metrics = [len(set(y_actual)), len(set(y_predicted)), len(tp), len(fn), fnr, len(fp), fpr]
    results = 'ActTot:{0}; PredTot:{1}; TP:{2}; FN:{3}; FNR:{4}; FP:{5}; FPR:{6}'.format(performance_metrics[0],
                                                                                         performance_metrics[1],
                                                                                         performance_metrics[2],
                                                                                         performance_metrics[3],
                                                                                         performance_metrics[4],
                                                                                         performance_metrics[5],
                                                                                         performance_metrics[6])
    return results, performance_metrics


def generates_performance_metrics(db, phenotype, results, gold_standard):
    """Generates performance metrics for a phenotype and writes each comparison to a separate tab in an Excel file.

    Args:
        db: A string naming a Google Big Query database.
        phenotype: A string naming the phenotype.
        results: A Pandas data frame of cohort results.
        gold_standard: A string naming the code set to be used as the gold standard.

    Returns:
        None.
    """

    # prepare file writer
    evaluation_results = []

    for grp_set in results.groups:
        print('Running Set - cohort:{cohort}, data:{data}, approach:{search}'.format(cohort=grp_set[0],
                                                                                     data=grp_set[1],
                                                                                     search=grp_set[2]))
        # re-group by code_sets
        grp_code_sets = results.get_group(grp_set).groupby(['code_set'])

        # set gold standard
        if gold_standard in [x for x in grp_code_sets.groups]:

            y_actual = grp_code_sets.get_group(gold_standard)['person_id']

            for c_set in [x for x in grp_code_sets.groups if x is not gold_standard]:

                # get results (TP/FP/FN/FNR/FPR)
                result_metrics = evaluates_performance(y_actual, grp_code_sets.get_group(c_set)['person_id'])
                evaluation_results.append([db, c_set, grp_set[0], grp_set[1], grp_set[2]] + result_metrics[1])

    # convert to pandas dataframe and save results
    merged_results = pd.DataFrame(dict(db=[x[0] for x in evaluation_results],
                                       code_set=[x[1] for x in evaluation_results],
                                       cohort=[x[2] for x in evaluation_results],
                                       clinical_data=[x[3] for x in evaluation_results],
                                       cohort_assignment=[x[4] for x in evaluation_results],
                                       actual_total=[x[5] for x in evaluation_results],
                                       predicted_total=[x[6] for x in evaluation_results],
                                       tp=[x[7] for x in evaluation_results],
                                       fn=[x[8] for x in evaluation_results],
                                       fnr=[x[9] for x in evaluation_results],
                                       fp=[x[10] for x in evaluation_results],
                                       fpr=[x[11] for x in evaluation_results]))
    # write data
    file_name = '_'.join(db.split('_')[:-1]) + '_' + phenotype.split('_')[0] + '_COHORT_METRICS.csv'
    merged_results.to_csv('temp_results/cohort_metrics/' + str(file_name), index=None, header=True, encoding='utf-8')

    return None


def main():
    code_sets = ['exact_none_self_stand_none_self',
                 'exact_none_self_stand_none_child',
                 'exact_none_self_stand_none_desc',
                 'exact_none_child_stand_none_self',
                 'exact_none_child_stand_none_child',
                 'exact_none_child_stand_none_desc',
                 'exact_none_desc_stand_none_self',
                 'exact_none_desc_stand_none_child',
                 'exact_none_desc_stand_none_desc',
                 'exact_syn_self_stand_none_self',
                 'exact_syn_self_stand_none_child',
                 'exact_syn_self_stand_none_desc',
                 'exact_syn_child_stand_none_self',
                 'exact_syn_child_stand_none_child',
                 'exact_syn_child_stand_none_desc',
                 'exact_syn_desc_stand_none_self',
                 'exact_syn_desc_stand_none_child',
                 'exact_syn_desc_stand_none_desc',
                 'fuzzy_none_self_stand_none_self',
                 'fuzzy_none_self_stand_none_child',
                 'fuzzy_none_self_stand_none_desc',
                 'fuzzy_none_child_stand_none_self',
                 'fuzzy_none_child_stand_none_child',
                 'fuzzy_none_child_stand_none_desc',
                 'fuzzy_none_desc_stand_none_self',
                 'fuzzy_none_desc_stand_none_child',
                 'fuzzy_none_desc_stand_none_desc',
                 'fuzzy_syn_self_stand_none_self',
                 'fuzzy_syn_self_stand_none_child',
                 'fuzzy_syn_self_stand_none_desc',
                 'fuzzy_syn_child_stand_none_self',
                 'fuzzy_syn_child_stand_none_child',
                 'fuzzy_syn_child_stand_none_desc',
                 'fuzzy_syn_desc_stand_none_self',
                 'fuzzy_syn_desc_stand_none_child',
                 'fuzzy_syn_desc_stand_none_desc']

    databases = ['CHCO_DeID_Oct2018', 'MIMICIII_OMOP_Mar2019']

    phenotypes = ['ADHD_COHORT_VARS',
                  'SICKLECELLDISEASE_COHORT_VARS',
                  'SLEEPAPNEA_COHORT_VARS',
                  'SYSTEMICLUPUSERYTHEMATOSUS_COHORT_VARS',
                  'APPENDICITIS_COHORT_VARS',
                  'CROHNSDISEASE_COHORT_VARS',
                  'STEROIDINDUCEDOSTEONECROSIS_COHORT_VARS',
                  'PEANUTALLERGY_COHORT_VARS',
                  'HYPOTHYROIDISM_COHORT_VARS']

    # load queries
    url = {x.split(';')[0]: x.split(';')[1] for x in open('resources/github_gists_cohort.txt', 'r').read().split('\n')}

    for db in databases:
        print('\n' + '*' * len('DATABASE: {db}'.format(db=db)))
        print('DATABASE: {db}'.format(db=db))
        print('*' * len('DATABASE: {db}'.format(db=db)))

        # loop over phenotypes
        for phenotype in phenotypes[4:]:

            # get queries
            queries = [url[key] for key in url if phenotype.split('_')[0].lower() in key.lower()]

            # run all queries for all code sets for each phenotype
            phenotype_cohorts = queries_gbq(phenotype, queries, code_sets, db)

            # generate results
            results_groupings = phenotype_cohorts.groupby(['cohort_type', 'clinical_data_type', 'cohort_assignment'])

            # generate performance metrics
            generates_performance_metrics(db, phenotype, results_groupings, 'exact_none_self_stand_none_self')


if __name__ == '__main__':
    main()
