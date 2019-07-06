##########################
# gscloud_to_gbquery.py
# Python 3.6.2
##########################

from google.cloud import storage
from google.cloud import bigquery


def storage_list(bucket_name):

    tables = []

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs(prefix='temp_results', delimiter=None)

    for blob in blobs:
        db = '_'.join(blob.name.split('/')[1].split('_')[0:3])

        table_vars = '_'.join(blob.name.split('/')[1].split('_')[3:]).split('.')[0].split('_')
        del table_vars[1]
        table_name = '_'.join(table_vars)

        file_name = blob.name

        tables.append([db, table_name, file_name])

    return tables


def main():

    bucket = input('Enter Google Cloud Bucket Name')
    # bucket = 'sandbox-tc.appspot.com'

    cloud_files = storage_list(bucket)

    # push data from the cloud to big query
    client = bigquery.Client()

    for data_file in cloud_files:

        table_ref = client.dataset(data_file[0]).table(data_file[1])

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        uri = "gs://sandbox-tc.appspot.com/" + str(data_file[2])

        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )
        # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        print("Loaded {} rows.".format(destination_table.num_rows))


if __name__ == '__main__':
    main()
