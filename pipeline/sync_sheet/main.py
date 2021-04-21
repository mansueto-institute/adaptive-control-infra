import os

import pandas

import google.auth
from google.cloud import storage
from googleapiclient.discovery import build

# cloud details
bucket_name = "adaptive-control-daily-pipeline"
blob_name   = "estimates/Rt_timeseries_india.csv"
filename    = blob_name.replace("estimates", "/tmp")

# sheet details
sheet_id = "17sDFb2DwplJX8A7bRdYvlEJdhRgsVpE44nQpNKWR6jM"

def sync_sheet(_):
    # download csv from cloud storage
    print("downloading csv")
    storage.Client()\
        .bucket(bucket_name)\
        .blob(blob_name)\
        .download_to_filename(filename)
    
    # load csv 
    print("loading csv")
    df = pandas.read_csv(filename)

    # write values to sheet 
    print("writing values to sheet")
    values = [list(a) for a in df[["state", "date", "Rt", "Rt_upper", "Rt_lower"]].values] 
    range_ = "Rt_timeseries_india!A2:E"

    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service  = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().values()\
        .update(
            spreadsheetId=sheet_id, 
            range=range_, 
            valueInputOption='USER_ENTERED', 
            body={"values":values})\
        .execute()
    print("response from sheets client", response)
