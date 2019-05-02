#########################
# big_query.py
# python 3.6.2
#########################


import apiclient
import datetime
import oauth2client.service_account
import pandas_gbq
import requests
# import pandas as pd

from datetime import datetime
from google.oauth2 import service_account


class GBQ(object):
    """Class creates a Google Big Query object which is used to query existing tables and create new tables.

    Attributes:
        key: A string containing the filepath to the Google API credentials.
        api: A string containing a url to the Google GBQ API.
        auth: An authorization client to access GBQ.
        gbq_service: A GBQ service object.
        auth2: An authorization client to query GBQ.
        project: A string containing a Google Cloud project name.
        database: A string containing the name of a GBQ database in the specified `project`.
    """

    def __init__(self, project, database):
        self.key = 'resources/programming/Google_API/secret_client_gbq.json'
        self.api = 'https://www.googleapis.com/auth/bigquery'
        self.auth = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name(self.key, self.api)
        self.gbq_service = apiclient.discovery.build('bigquery', 'v2', credentials=self.auth)
        self.auth2 = service_account.Credentials.from_service_account_file(self.key)
        self.project = project
        self.database = database

    def table_info(self):
        """Returns the name of all of the tables in a database.

        Returns:
            A list of the table names in a database.
        """

        table_list = self.gbq_service.tables().list(projectId=self.project, datasetId=self.database).execute()
        tables = [x['id'].split('.')[-1] for x in table_list['tables']]

        print('Retrieved {0} tables from {1}'.format(len(tables), self.database))

        return tables

    def create_table(self, table_name, data):
        """Creates a new GBQ table from an input pandas data frame.

        Args:
            table_name: A string that contains the name of a table.
            data: A pandas data frame.

        Returns:
            None.
        """

        pandas_gbq.to_gbq(data, str(self.database) + "." + str(table_name),
                          project_id=self.project,
                          if_exists='replace',
                          progress_bar=True,
                          credentials=self.auth2)

        print('Created new table: {0} in {1}'.format(table_name, self.database))

    def gbq_query(self, url, str_args):
        """Queries a GBQ table and returns the output.

        Args:
            url: A string that contains a URL.
            str_args: A tuple of arguments to pass into an SQL query.

        Returns:
            A pandas data frame.
        """

        start = datetime.now()
        print('Started processing query: {}'.format(start))
        query = requests.get(url, allow_redirects=True).text.format(*str_args)

        results = pandas_gbq.read_gbq(query, dialect='standard', project_id=self.project, credentials=self.auth2)

        finish = datetime.now()
        print("Finished processing query: {}".format(finish))

        duration = finish-start
        time_diff = round(duration.total_seconds(), 2)
        print('Query returned: {0} results in {1} seconds \n'.format(len(results), time_diff))

        return results
