##########################
# gscloud_to_gbquery.py
# Python 3.6.2
##########################

import warnings

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

warnings.filterwarnings("ignore")
# TODO: Update authentication to use storage account rather than Google Cloud SDK.


def parses_gsc_filepath(bucket_name, bucket_directory):
    """Extract and parses file paths from a Google Cloud Storage directory into a nested list.

       input: temp_results/CHCO_DeID_Oct2018_APPENDICITIS_Condition_COHORT_VARS.csv

    Args:
        bucket_name: A string naming the name of a Google Cloud Storage Bucket.
        bucket_directory: A string naming a Google Cloud Storage directory.

    Returns:
        A nested list of information needed to create Google BigQuery tables. For example:

        [['Google BigQuery [DB]', 'Google BigQuery [DB].[TABLE_NAME]', gscloud file path/file.csv]]
    """

    tables = []

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # retrieve relevant file paths
    blobs = bucket.list_blobs(prefix=bucket_directory, delimiter=None)

    for blob in blobs:

        if '.DS_Store' not in blob.name:

            # database name
            db = '_'.join(blob.name.split('/')[-1].split('_')[0:3])

            # table name
            table_vars = '_'.join(blob.name.split('/')[-1].split('_')[3:]).split('.')[0].split('_')
            del table_vars[1:3]
            table_name = '_'.join(table_vars)

            # gs cloud file to write
            file_name = blob.name

            tables.append([db, table_name, file_name])

    return tables


def loads_gbq_table_data(gcs_data_list, bucket_name, table_action=bigquery.WriteDisposition.WRITE_APPEND):
    """loads data from a list to Google Big Query tables.

    Args:
        gcs_data_list: A nested list of information needed to create Google BigQuery tables.
        bucket_name: A string naming the name of a Google Cloud Storage Bucket.
        table_action: A biqquery object which specifies how data should be written to tables. The Default parameter
            settings configured to append data to existing tables rather than overwrite it. To only write data if table
            is empty, update table_action to 'bigquery.WriteDisposition.WRITE_EMPTY' and to overwrite existing data
            change table_action to 'bigquery.WriteDisposition.WRITE_TRUNCATE'.

    Returns:
        None.

    Raises:
        An error occurred if the table trying to be written already exists.
    """

    client = bigquery.Client()

    # for data_file in gcs_data_list:
        # # make sure table does not already exist
        # try:
        #     client = bigquery.Client()
        #     dataset = client.dataset(data_file[0])
        #     table_ref = dataset.table(data_file[1])
        #     client.get_table(table_ref)
        #
        # except NotFound:

    for data_file in gcs_data_list:

        print('\n' + '**' * 25)
        table_data_type = '_'.join(data_file[2].split('/')[-1].split('.')[0].split('_')[-4:-2])
        print('Writing {table}:{data_type} to {db}'.format(table=data_file[1],
                                                           data_type=table_data_type,
                                                           db=data_file[0]))

        # configure job specifications
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = table_action
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
                                              'gs://' + str(bucket_name) + '/' + str(data_file[2]),
                                              client.dataset(data_file[0]).table(data_file[1]),
                                              job_config=job_config
                                              )

        # make API request and print rows to confirm successful upload
        print('Starting load job: {}'.format(load_job.job_id))
        load_job.result()
        print('Load job finished.')
        print('Loaded {} rows'.format(client.get_table(client.dataset(data_file[0]).table(data_file[1])).num_rows))

    return None


def main():

    print('\n')
    gscloud_bucket = input('Enter Google Cloud Storage Bucket Name: ')
    gscloud_bucket_dir = input('Enter Google Cloud Bucket Name: ')

    # gscloud_bucket = 'sandbox-tc.appspot.com'
    # gscloud_bucket_dir = 'PheKnowVec_1.0_09August2019/temp_results'

    # get data from Google Cloud Storage
    # cloud_files = parses_gsc_filepath('sandbox-tc.appspot.com', 'PheKnowVec_1.0_09August2019/temp_results/')
    cloud_files = parses_gsc_filepath(gscloud_bucket, gscloud_bucket_dir)
    cloud_files_update = [x for x in cloud_files if 'HYPOTHYROIDISM_COHORT_VARS' not in x]

    # push data to Google Big Query
    loads_gbq_table_data(cloud_files_update, gscloud_bucket)


if __name__ == '__main__':
    main()
