#############################################################################################
# GBQConnect.py
# Purpose: script creates a connection to OMOP de-identified data stored in Google BigQuery
# version 1.0.0
#############################################################################################


import pandas as pd
import pygsheets



# use creds to create a client to interact with the Google Drive API
creds = ServiceAccountCredentials.from_json_keyfile_name('resources/programming/Google_API/PheKnowVec-3d60a29a7dc4.json',
                                                         'https://www.googleapis.com/auth/drive')
gs_client = gspread.authorize(creds)
wks = gs_client.open("Phenotype Definitions")
ADHD_wks = wks.worksheet("ADHD_179")

dataframe = pd.DataFrame(wks.worksheet("ADHD_179").get_all_records())










