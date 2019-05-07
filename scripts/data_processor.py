#########################
# data_processor.py
# python 3.6.2
#########################


import gspread
import gspread_dataframe as gd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import time

from oauth2client.service_account import ServiceAccountCredentials

from scripts.big_query import *


class GSProcessor(object):
    """Class creates a data frame from a Google Sheet and contains methods to manipulate the data it contains.

    Attributes:
        auth: A string containing a file path to the json file containing Google API information.
        cred: A Service Account Credential object.
        spreadsheet info: A list where the first item is a string that contains the name of a Google Sheet and the second
        item is a string that contains the name of a tab in the `worksheet`.
        spreadsheet: A authorized gspread object.
        worksheet: An empty string to store a specific worksheet in  a Google Sheet.
        data: An empty string to store GoogleSheet data.
        sheet_dic: An empty dictionary that stores GoogleSheet metadata.
    """

    def __init__(self, spreadsheet_info):
        self.auth = 'resources/programming/Google_API/secret_client_gs.json'
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive',
                      'https://www.googleapis.com/auth/drive.file',
                      'https://www.googleapis.com/auth/spreadsheets']
        self.cred = ServiceAccountCredentials.from_json_keyfile_name(self.auth, self.scope)
        self.client = gspread.authorize(self.cred)
        self.email_address = 'callahantiff@gmail.com'
        self.spreadsheet_info = spreadsheet_info
        self.spreadsheet = self.client.open(self.spreadsheet_info[0])
        self.worksheet = ''
        self.data = pd.DataFrame()
        self.sheet_dic = dict()

    def data_download(self):
        """Connects to Google Sheets, downloads a spreadsheet, and converts the downloaded results to a pandas data
        frame."""

        temp_data = gd.get_as_dataframe(self.spreadsheet.worksheet(self.spreadsheet_info[1]))

        if len(temp_data) == 0:
            raise ValueError('Error - {0} does not contain any data!'.format(str(self.spreadsheet_info[0]) +
                                                                             "_" + str(self.spreadsheet_info[1])))
        else:
            self.data = temp_data
            print('Downloading data from Google Sheet: {0}, Tab: {1}\n'.format(self.spreadsheet_info[0],
                                                                               self.spreadsheet_info[1]))

    def authorize_client(self):

        self.auth = 'resources/programming/Google_API/secret_client_gs.json'
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive',
                      'https://www.googleapis.com/auth/drive.file',
                      'https://www.googleapis.com/auth/spreadsheets']
        self.cred = ServiceAccountCredentials.from_json_keyfile_name(self.auth, self.scope)
        self.client = gspread.authorize(self.cred)

        return self.client

    def get_data(self):
        return self.data

    def get_spreadsheet(self):
        return self.spreadsheet

    def get_worksheet(self):
        return self.worksheet

    def set_spreadsheet(self, spreadsheet_id):
        self.spreadsheet = self.client.open_by_key(spreadsheet_id)

    def set_worksheet(self, worksheet_name):
        """Takes a string as an argument and changes active tab in google sheets.

        Args:
            worksheet_name: A string containing the name of a tab in the current Google Sheet.

        Returns:
             None.
        """
        self.worksheet = self.spreadsheet.worksheet(worksheet_name)
        print('Updated, switched to Google Sheet: {0}, Tab: {1}\n'.format(self.get_spreadsheet().title, worksheet_name))

    def create_worksheet(self, spreadsheet_name):
        """Takes a string as an argument and changes active tab in google sheets.

        Args:
            spreadsheet_name: A string containing the name of a tab in the current GoogleSheet.

        Returns:
             None.
        """

        self.get_spreadsheet().add_worksheet(title=spreadsheet_name, rows=1, cols=1)

    def count_spreadsheet_cells(self, spreadsheet_name):
        """counts the number of cells in a worksheet.

        Args:
            spreadsheet_name: A string containing the name of a tab in the current GoogleSheet.

        Returns:
            cell_count: An integer that indicates the total number of cells in a spreadsheet.
        """

        cell_count = 0

        spreadsheet = self.client.open(spreadsheet_name)
        tabs = [str(x).split("'")[1] for x in list(spreadsheet.worksheets())]

        for tab in tabs:
            wks = spreadsheet.worksheet(tab)
            cell_count += wks.row_count*wks.col_count

        return cell_count

    def list_spreadsheet_tabs(self, spreadsheet_name):
        """Takes the name of GoogleSheet and returns all named tabs within that spreadsheet.

        Args:
            spreadsheet_name: A string containing the name of a GoogleSheet.

        Returns:
             A list of tabs in a spreadsheet.
        """

        spreadsheet = self.client.open(spreadsheet_name)
        tabs = [str(x).split("'")[1] for x in list(spreadsheet.worksheets())]

        return tabs

    def create_spreadsheet(self, spreadsheet_name, email_address):
        """Takes a string that contains a spreadsheet name and a string that contains the email address of who the
        spreadsheet should be shared with.

        Args:
            spreadsheet_name: A string that contains a spreadsheet name.
            email_address: A a string that contains an email address.

        Returns:
            None.
        """
        sh = self.client.create(spreadsheet_name)
        sh.share(email_address, perm_type='user', role='writer')

    def write_data(self, spreadsheet_name, tab_name, results):
        """Writes a pandas dataframe to a specific tab in a GoogleSheet. Data is only written if the tab does not
        already
        exists in the spreadsheet.

        Args:
            spreadsheet_name: A string containing the name of a GoogleSheet spreadsheet.
            tab_name:A string containing the name of a tab within a GoogleSheet spreadsheet.
            results: A pandas dataframe.

        Returns:
            None.

        """

        # write results
        spreadsheets = {sheet.title: sheet.id for sheet in self.client.openall()}
        if spreadsheet_name in spreadsheets.keys():
            sheet_id = spreadsheets[spreadsheet_name]

            # re-instantiate object
            self.authorize_client()

            # open spreadsheet and write data
            self.client.open_by_key(sheet_id)

            # search tabs in sheet, if not there write data
            if tab_name not in self.list_spreadsheet_tabs(spreadsheet_name):

                # set spreadsheet
                self.set_spreadsheet(sheet_id)
                time.sleep(5)

                # create worksheet
                self.create_worksheet(tab_name)
                self.set_worksheet(tab_name)
                time.sleep(5)

                # write data
                gd.set_with_dataframe(self.worksheet, results)

        else:
            # re-instantiate object
            self.authorize_client()

            # create spreadsheet
            self.create_spreadsheet(spreadsheet_name, self.email_address)
            self.set_spreadsheet({sheet.title: sheet.id for sheet in self.client.openall()}[spreadsheet_name])
            time.sleep(5)

            # create worksheet
            self.create_worksheet(tab_name)
            self.set_worksheet(tab_name)
            time.sleep(5)

            # write data
            gd.set_with_dataframe(self.get_worksheet(), results)

        return None

    def set_data(self, new_data):
        """Takes a pandas data frame as an argument and uses it to update the instance's data.

        Args:
            new_data: A string containing the name of a tab in the current Google Sheet.

        Returns:
             None.
        """
        self.data = new_data
        print('Updated Instance -- Set New Pandas Dataframe')

    def count_merger(self, databases, merged_results):
        """Function that merges two pandas data frames together and reformats the merged dataframe columns.

        Args:
            databases: A list of Google BigQuery databases.
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
        col_set2 = set(list(self.data)).intersection(set(list(merged_agg)))
        merged_full = pd.merge(left=self.data, right=merged_agg, how='outer', left_on=list(col_set2),
                               right_on=list(col_set2))

        # replace rows with no counts, stored as 'NaN' with zero
        merged_full[[db1_name, db2_name]] = merged_full[[db1_name, db2_name]].fillna(0)
        return merged_full.drop_duplicates()

    def domain_occurrence(self, project, url=None, databases=None):
        """Function takes a pandas dataframe and a string containing a url which redirects to a GitHubGist SQL query.
        The
        function formats a query and runs it against the input database(s). This function assumes that it will be passed
        two relational databases as an argument.

        Args:
            project: A Google BigQuery project.
            url: A string containing a url which redirects to a GitHub Gist SQL query.
            databases: A list where the items are 'None' or a list of strings that are database names.

        Returns:
            A pandas dataframe.
        """

        merged_res = []

        for db_ in databases:
            print('\n' + '#' * 50 + '\n' + 'Running query against {0}'.format(db_))
            db = GBQ(project, db_)

            # generate and run queries against SQL database
            query_results = []
            batch = self.data.groupby(np.arange(len(self.data)) // 15000)

            for name, group in batch:
                print('\n Processing chunk {0} of {1}'.format(name + 1, batch.ngroups))

                sql_args = self.code_format(group, ['code'], '', url.split('/')[-1])
                res = db.gbq_query(url, (db_, *sql_args))
                time.sleep(5)
                query_results.append(res.drop_duplicates())

            merged = pd.concat(query_results).drop_duplicates()

            # rename column
            merged.rename(columns={'occ_count': str(db_.split('_')[0]) + '_count'}, inplace=True)
            merged_res.append(merged)

        # merge results from the input databases together
        return self.count_merger(databases, merged_res)

    def regular_query(self, input_source, project, url, gbq_database):
        """Function generates a SQL query and runs it against a Google BigQuery database. The returned results are
        then used in a second query designed to retrieve

        Args:
            input_source: A list of items needed to query a database:
                - A string that contains the name of a query
                - A string that contains a character and is used to indicate whether or not a modifier should be used.
                - A list of strings that represent columns in a pandas dataframe:
                (1) a string that indicates if the query uses input strings or codes
                (2) a string that indicates the name of the column that holds the source codes or strings
                (3) a string that indicates the name of the column that holds the source domain or source vocabulary
                (4) 'None' or a list of strings that are database names
                (5) 'None' or a string that is used to signal whether or not a sub-query should be performed.
            project: A Google BigQuery project.
            url: A dictionary of urls; each url represents an SQL query.
            gbq_database: A string containing the name of a Google Big Query database.

            Example of input_source:
                ['wildcard_match', '%', ['str', 'source_id', 'source_domain', None, None]]

        Returns:
            A pandas dataframe.

        """
        # initialize GBQ object
        gbq_db = GBQ(project, gbq_database)

        # create an empty list to store results
        query_results = []
        batch = self.data.groupby(np.arange(len(self.data)) // 15000)

        for name, group in batch:
            print('\n Processing chunk {0} of {1}'.format(name + 1, batch.ngroups))
            sql_args = self.code_format(group, input_source[2], input_source[1])
            res = gbq_db.gbq_query(url[input_source[0]], (gbq_database, *sql_args))
            time.sleep(5)
            query_results.append(res.drop_duplicates())

        # get occurrence counts
        cont_results = pd.concat(query_results).drop_duplicates()

        # add back source_string (only for standard queries)
        if 'source_string' in cont_results:
            merged_results = cont_results.copy()
        else:
            merged_results = pd.merge(left=self.data[['source_code', 'source_string']].drop_duplicates(),
                                      right=cont_results, how='right', on='source_code').drop_duplicates()

        if 'code_count' not in input_source[2] or len(merged_results) == 0:
            # we don't want occurrence counts for source string queries or when no query results are returned
            print('There are {0} unique rows in the results dataframe'.format(len(merged_results)))
            return merged_results

        else:
            # reset data to results before getting counts
            self.set_data(merged_results)

            # get occurrence counts
            return self.domain_occurrence(project, url[input_source[2][4]], input_source[2][5])

    @staticmethod
    def code_format(data, input_list, mod='', data_type=None):
        """Extract information needed to in an SQL query from a pandas data frame and return needed information as a
        list of sets.

        Args:
            data: A pandas data frame.
            input_list: A list of strings that represent columns in a pandas dataframe. The function assumes that
                the list contains the following:
                    (1) a string that indicates if the query uses input strings or codes
                    (2) a string that indicates the name of the column that holds the source codes or strings
                    (3) a string that indicates the name of the column that holds the source domain or source vocabulary
                    (4) 'None' or a list of strings that are database names
            mod: A string that indicates whether or not a modifier should be used.
            data_type: A string containing the name of a query.

        Returns:
             When input_source includes 'string', then list of joined sets is returned, where each set contains an SQL
             parameter. For example:

            ('"WHEN lower(concept_name) LIKE '%clonazepam%' THEN '%clonazepam%'', '"Drug"')

            When input_source includes 'code', then list of joined sets is returned, where each set contains an SQL
             parameter. For example:

            ('"348.1", "348.2", "348.3"', '"ICD9CM"', '"SNOMED"')

        Raises:
            An error occurs when the input data frame does not contain required data (i.e. source codes, source vocab,
            and standard vocabulary information.
        """

        # get data
        tables = ['drug_exposure', 'condition_occurrence', 'procedure_occurrence', 'observation', 'measurement']

        if 'code' not in input_list[0]:
            if '%' not in mod:
                format_mod = lambda x: "WHEN lower(c.concept_name) LIKE '{0}' THEN '{0}'".format(x.strip('"').lower())
            else:
                format_mod = lambda x: "WHEN lower(c.concept_name) LIKE '%{0}%' THEN '{0}'".format(x.strip('"').lower())

            source1 = set(list(data[input_list[1]].apply(format_mod)))
            source2 = set(list(data[input_list[2]]))
            res = '\n'.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"'

        else:
            # to get occurrence counts
            if data_type is not None:
                source1 = set(list(data['standard_code']))
                source2 = set(list(data['standard_vocabulary']))
                source3 = set(list(data['standard_domain'])).pop()
                table_name = tables[[i for i, s in enumerate(tables) if source3.lower() in s][0]]
                res = ','.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"', '"'\
                      + source3 + '"', table_name, table_name.split('_')[0]

            else:
                source1 = set(list(data[input_list[1]]))
                source2 = set(list(data[input_list[2]]))
                source3 = set(list(data[input_list[3]])).pop()

                # to get source concept_codes
                if len(input_list) < 6:
                    res = ','.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"', '"' \
                          + source3 + '"'

                # to get standard terms
                else:
                    source4 = input_list[6]
                    res = ','.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"', '"'\
                          + source3 + '"', '"' + source4 + '"'

        if not len(source1) and len(source2) >= 1:
            raise ValueError('Error - check your input data, important variables may be missing')
        else:
            return res

    @staticmethod
    def descriptive(data, plot_title, plot_x_axis, plot_y_axis):
        """Function takes a pandas data frame, and three strings that contain information about the data frame. The
        function then derives and prints descriptive statistics and histograms.

        Args:
            data: a pandas dataframe.
            plot_title: A string containing a title for the histogram.
            plot_x_axis: A string containing a x-axis label for the histogram.
            plot_y_axis: A string containing a y-axis label for the histogram.

        Returns:
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
        print('CHCO: The Average Occurrences per Standard Code is: {0} \n'.format(num_c / denom_c))

        # MIMIC
        print(plt_dat_mimic[['standard_code']].describe())
        num_m = plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_count'] > 0.0]['MIMICIII_count'].sum()
        denom_m = len(set(list(plt_dat_mimic.loc[plt_dat_mimic['MIMICIII_count'] > 0.0]['standard_code'])))
        print('MIMICIII: There are {0} Unique Standard Codes with Occurrence > 0'.format(denom_m))
        print('MIMICIII: The Average Occurrences per Standard Code is: {0} \n'.format(num_m / denom_m))

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
