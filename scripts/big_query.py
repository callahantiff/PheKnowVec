#########################
# big_query.py
# python 3.6.2
#########################


import apiclient
import datetime
import pandas_gbq

from datetime import datetime
from google.oauth2 import service_account
import oauth2client.service_account
from pandas_gbq import exceptions

# TODO: improve handling of API errors, the approach works but adds in some redundant rer-activation of the API client.


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
        self.key = 'resources/programming/Google_API/sandbox-tc-b0a5e4cd1d8e.json'
        self.api = 'https://www.googleapis.com/auth/bigquery'
        self.auth = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name(self.key, self.api)
        self.gbq_service = apiclient.discovery.build('bigquery', 'v2', credentials=self.auth)
        self.auth2 = service_account.Credentials.from_service_account_file(self.key)
        self.project = project
        self.database = database

    def get_authorization(self):
        self.auth = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name(self.key, self.api)
        self.gbq_service = apiclient.discovery.build('bigquery', 'v2', credentials=self.auth)
        self.auth2 = service_account.Credentials.from_service_account_file(self.key)

        return None

    def table_info(self):
        """Returns the name of all of the tables in a database.

        Returns:
            A list of the table names in a database.
        """

        table_list = self.gbq_service.tables().list(projectId=self.project, datasetId=self.database).execute()
        tables = [x['id'].split('.')[-1] for x in table_list['tables']]

        # print('Retrieved {0} tables from {1}'.format(len(tables), self.database))

        return tables

    def create_table(self, table_name, data, modify_decision):
        """Creates a new GBQ table from an input pandas data frame.

        Args:
            table_name: A string that contains the name of a table.
            data: A pandas data frame.
            modify_decision: A string specifying whether data should be "appended" to an existing table, "replace" an
            data in an existing table or "fail" and add nothing to it.

        Returns:
            None.
        """

        pandas_gbq.to_gbq(data, str(self.database) + "." + str(table_name),
                          project_id=self.project,
                          if_exists=modify_decision,
                          progress_bar=True,
                          chunksize=10000,
                          credentials=self.auth2)

        print('Created new table: {0} in {1}'.format(table_name, self.database))

    def gbq_query(self, query):
        """Queries a GBQ table and returns the output.

        Args:
            query: A formatted string containing a SQL query.

        Returns:
            A pandas data frame.
        """

        start = datetime.now()
        # print('Started processing query: {}'.format(start))

        try:
            results = pandas_gbq.read_gbq(query, dialect='standard', project_id=self.project,
                                          credentials=self.auth2)

        except pandas_gbq.exceptions.AccessDenied:
            self.get_authorization()

            results = pandas_gbq.read_gbq(query, dialect='standard', project_id=self.project,
                                          credentials=self.auth2)

        finish = datetime.now()
        # print("Finished processing query: {}".format(finish))

        duration = finish-start
        time_diff = round(duration.total_seconds(), 2)
        # print('Query returned: {0} results in {1} seconds'.format(len(results), time_diff))

        return results
