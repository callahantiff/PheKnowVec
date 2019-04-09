#########################
# data_processor.py
# version 1.0.0
# python 3.6.2
#########################


import gspread
import gspread_dataframe as gd
import pandas as pd

from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from progressbar import ProgressBar, FormatLabel, Percentage, Bar


class GSProcessor(object):
    """Class creates a data frame from a Google Sheet and contains methods to manipulate the data it contains.

    Attributes:
        auth: A string containing a file path to the json file containing Google API information.
        cred: A Service Account Credential object.
        sheet_info: A list where the first item is a string that contains the name of a Google Sheet and the second
        item is a string that contains the name of a tab in the `worksheet`.
        sheet: A authorized gspread object.
        worksheet: An empty string to store a specific worksheet in  a Google Sheet.
        data: An empty string to store Google Sheet data.
    """

    def __init__(self, sheet_info):
        self.auth = 'resources/programming/Google_API/secret_client_gs.json'
        self.cred = ServiceAccountCredentials.from_json_keyfile_name(self.auth, 'https://www.googleapis.com/auth/drive')
        self.sheet_info = sheet_info
        self.sheet = gspread.authorize(self.cred).open(self.sheet_info[0])
        self.worksheet = ''
        self.data = ''

    def data_download(self):
        """Connects to Google Sheets, downloads a spreadsheet and saves it to a pandas data frame."""

        temp_data = gd.get_as_dataframe(self.sheet.worksheet(self.sheet_info[1]))

        if len(temp_data) == 0:
            raise ValueError('Error - {0} does not contain any data!'.format(str(self.sheet_info[0]) +
                                                                             "_" + str(self.sheet_info[1])))
        else:
            self.data = temp_data
            print('Downloading data from Google Sheet: {0}, Tab: {1}\n'.format(self.sheet_info[0], self.sheet_info[1]))

    def get_data(self):
        return self.data

    def get_sheet(self):
        return self.sheet

    def get_worksheet(self):
        return self.worksheet

    def create_worksheet(self, sheet_name):
        """Takes a string as an argument and changes active tab in google sheets.

                Args:
                    sheet_name: A string containing the name of a tab in the current Google Sheet.

                Return:
                     None.
                """

        self.get_sheet().add_worksheet(title=sheet_name, rows=1, cols=1)

    def set_worksheet(self, sheet_name):
        """Takes a string as an argument and changes active tab in google sheets.

        Args:
            sheet_name: A string containing the name of a tab in the current Google Sheet.

        Return:
             None.
        """
        self.worksheet = self.sheet.worksheet(sheet_name)
        print('Updated, switched to Google Sheet: {0}, Tab: {1}\n'.format(self.sheet_info[0], sheet_name))

    @staticmethod
    def code_format(codes):
        """Extract information needed to in an SQL query from a pandas data frame and return needed information as a
        list of sets. This function assumes that the input data contains specific columns: 'Input_Type',
        'Source_Code', 'Source_Vocabulary', and 'Standard_Vocabulary'.

        Args:
             codes: A pandas data frame.

        Returns:
            A tuple of joined sets, where each set contains an SQL parameter. For example:

            ('"348.1", "348.2", "348.3"', '"ICD9CM"', '"SNOMED"')

        Raises:
            An error occurs if the data frame does not contain the mandatory columns.
            An error occurs when the input data frame does not contain required data (i.e. source codes,
            source vocab, and standard vocabulary information.
        """

        if 'input_type' and 'source_code' and 'source_vocabulary' and 'standard_vocabulary' not in list(codes):
            raise ValueError('Error - data does not contain the mandatory columns. Please make sure your data frame '
                             'includes the following columns: input_type, source_code, source_vocabulary, and '
                             'standard_vocabulary')
        else:

            source_code = set()
            source_vocab = set()
            standard_vocab = set()

            code_data = codes.groupby('input_type').get_group('Code')

            start = datetime.now()
            print('\n Started processing source codes: {}'.format(start))
            widgets = [Percentage(), Bar(), FormatLabel('(elapsed: %(elapsed)s)')]
            pbar = ProgressBar(widgets=widgets, maxval=len(code_data))

            for row, val in pbar(code_data.iterrows()):
                source_code.add(val['source_code'])
                source_vocab.add(val['source_vocabulary'])
                standard_vocab.add(val['standard_vocabulary'])

            pbar.finish()
            finish = datetime.now()
            print("Finished processing source codes: {}".format(finish))

            # verify we have results
            if not len(source_code) and len(source_vocab) and len(standard_vocab) >= 1:
                raise ValueError('Error - check your data file, important variables may be missing')
            else:
                duration = finish - start
                time_diff = round(duration.total_seconds(), 2)
                print('Processed {0} codes in {1} seconds \n'.format(len(source_code), time_diff))

                return (','.join(map(str, source_code)),
                        '"' + '","'.join(map(str, source_vocab)) + '"',
                        '"' + '","'.join(map(str, standard_vocab)) + '"')

    @staticmethod
    def string_format(strings):
        """Extract information needed to in an SQL query from a pandas data frame and return needed information as a
        list of sets. This function assumes that the input data contains specific columns: 'Input_Type',
        'Source_Code', 'Source_Vocabulary', and 'Standard_Vocabulary'.

        Args:
             strings: A pandas data frame.

        Returns:
            A tuple of joined sets, where each set contains an SQL parameter. For example:

            ('"WHEN lower(concept_name) LIKE '%clonazepam%' THEN '%clonazepam%'', '"Drug"')

        Raises:
            An error occurs if the data frame does not contain the mandatory columns.
            An error occurs when the input data frame does not contain required data (i.e. source strings and source
            domain information.
        """
        source_string = []
        source_domain = set()

        if 'input_type' and 'source_domain' and 'source_code' not in list(strings):
            raise ValueError('Error - data does not contain the mandatory columns. Please make sure your data frame '
                             'includes the following columns: input_type, source_domain, and source_code')
        else:

            string_data = strings.groupby('input_type').get_group('String')

            start = datetime.now()
            print('\n Started processing source strings: {}'.format(start))
            widgets = [Percentage(), Bar(), FormatLabel('(elapsed: %(elapsed)s)')]
            pbar = ProgressBar(widgets=widgets, maxval=len(string_data))

            for row, val in pbar(string_data.iterrows()):
                source_domain.add(val['source_domain'])
                source_string.append("WHEN lower(concept_name) LIKE {0} THEN '{0}'".format(val['source_code'].lower()))

            pbar.finish()
            finish = datetime.now()
            print("Finished processing source strings: {}".format(finish))

            # verify we have results
            if not len(source_string) and len(source_domain) >= 1:
                raise ValueError('Error - check your data file, important variables may be missing')
            else:
                duration = finish - start
                time_diff = round(duration.total_seconds(), 2)
                print('Processed {0} strings in {1} seconds \n'.format(len(source_string), time_diff))

                return '\n '.join(map(str, set(source_string))), '"' + '","'.join(map(str, source_domain)) + '"'

    @staticmethod
    def result_merger(data, results, merge_cols, keep_cols):
        """Process results of querying Google Big Query and merge them with data extracted from a Google Sheet.

        Args:
            data: A pandas data frame containing data from a Google Sheet.
            results: A pandas data frame containing data from running a query against GBQ.
            merge_cols: A list of columns to merge results and data with.
            keep_cols: A list of columns to keep in the resulting merged data frame.

        Return:
            A pandas data frame containing the original data merged with the results from running a query against GBQ.
        """

        # merge strings to existing data
        merged_data = pd.merge(left=data, right=results, how='left',
                               left_on=[merge_cols[0], merge_cols[1]],
                               right_on=[merge_cols[2], merge_cols[3]])

        for row, val in merged_data.iterrows():
            if val['input_type'] == 'String' and pd.isnull(val[str(keep_cols[1]) + '_y']) is False:
                val[str(keep_cols[0]) + '_x'] = val[str(keep_cols[0]) + '_y']
                val[str(keep_cols[1]) + '_x'] = val[str(keep_cols[1]) + '_y']
                val[str(keep_cols[2]) + '_x'] = val[str(keep_cols[2]) + '_y']

        # rename columns
        merged_data.rename(columns={str(keep_cols[0]) + '_x': str(keep_cols[0]).split('_x')[0],
                                    str(keep_cols[1]) + '_x': str(keep_cols[1]).split('_x')[0],
                                    str(keep_cols[2]) + '_x': str(keep_cols[2]).split('_x')[0]}, inplace=True)

        # drop old columns
        merged_data = merged_data.drop([x for x in list(merged_data) if '_y' in x], axis=1)

        return merged_data
