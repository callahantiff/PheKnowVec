#########################
# data_processor.py
# version 1.0.0
# python 3.6.2
#########################


import gspread
import gspread_dataframe as gd
import pandas as pd

from oauth2client.service_account import ServiceAccountCredentials


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
        self.data = pd.DataFrame()

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

    def create_spreadsheet(self, spreadsheet_name, email_address):
        """Takes a string that contains a spreadsheet name and a string that contains the email address of who the
        spreadsheet should be shared with.

        Args:
            spreadsheet_name: A string that contains a spreadsheet name.
            email_address: A a string that contains an email address.

        Return:
            None.
        """
        sh = gspread.authorize(self.cred).create(spreadsheet_name)
        sh.share(email_address, perm_type='user', role='writer')

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
    def sheet_writer(spreadsheet, results):
        """Takes an instantiated class and a pandas dataframe and writes the data to the location specified by the
        class.

        Args:
            spreadsheet: A string that contains an instance of the class.
            results: A pandas dataframe.

        Return:
             None
        """

        gd.set_with_dataframe(spreadsheet.get_worksheet(), results)

    def set_data(self, new_data):
        """Takes a pandas data frame as an argument and uses it to update the instance's data.

        Args:
            new_data: A string containing the name of a tab in the current Google Sheet.

        Return:
             None.
        """
        self.data = new_data
        print('Updated Instance -- Set New Pandas Dataframe')

    @staticmethod
    def code_format(temp_data, input_source, mod='', data_type=None):
        """Extract information needed to in an SQL query from a pandas data frame and return needed information as a
        list of sets.

        Args:
            temp_data: A pandas data frame.
            input_source: A list of strings that represent columns in a pandas dataframe. The function assumes that
                the list contains the following:
                    (1) a string that indicates if the query uses input strings or codes
                    (2) a string that indicates the name of the column that holds the source codes or strings
                    (3) a string that indicates the name of the column that holds the source domain or source vocabulary
                    (4) 'None' or a list of strings that are database names
            data_type: A string containing the name of a query.
            mod: A string that indicates whether or not a modifier should be used.

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

        if 'code' not in input_source[0] and mod == '':
            format_nomod = lambda x: "WHEN lower(c.concept_name) LIKE '{0}' THEN '{0}'".format(x.strip('"').lower())
            source1 = set(list(temp_data[input_source[1]].apply(format_nomod)))
            source2 = set(list(temp_data[input_source[2]]))
            res = '\n'.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"'

        elif 'code' not in input_source[0] and mod != '':
            format_mod = lambda x: "WHEN lower(c.concept_name) LIKE '%{0}%' THEN '{0}'".format(x.strip('"').lower())
            source1 = set(list(temp_data[input_source[1]].apply(format_mod)))
            source2 = set(list(temp_data[input_source[2]]))
            res = '\n'.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"'

        else:
            if data_type is not None:
                table_name = tables[[i for i, s in enumerate(tables) if set(temp_data['source_domain']).pop() in s][0]]
                source1 = set(list(temp_data[input_source[1]]))
                source2 = set(list(temp_data[input_source[2]]))
                res = ','.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"' + table_name + table_name

            else:
                source1 = set(list(temp_data[input_source[1]]))
                source2 = set(list(temp_data[input_source[2]]))
                res = ','.join(map(str, source1)), '"' + '","'.join(map(str, source2)) + '"'

        if not len(source1) and len(source2) >= 1:
            raise ValueError('Error - check your input data, important variables may be missing')
        else:
            return res
