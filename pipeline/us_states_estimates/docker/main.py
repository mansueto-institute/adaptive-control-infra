from pathlib import Path
from typing  import Dict, Optional, Sequence, Tuple, Callable
from tqdm    import tqdm
from io      import StringIO

import google.auth
from google.cloud import storage
from googleapiclient.discovery import build

from adaptive.utils      import cwd
from adaptive.estimators import gamma_prior
from adaptive.smoothing  import notched_smoothing

from etl import import_clean_smooth_cases, get_new_rt_live_estimates
from rtlive_old_model import run_rtlive_old_model
from luis_model import run_luis_model

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import subprocess


# PARAMETERS
CI               = 0.95
smoothing_window = 7
rexepath         = 'C:\\Program Files\\R\\R-3.6.1\\bin\\'

# CLOUD DETAILS 
bucket_name = "us-states-rt-estimation"

# SHEET SYNCING INFO
sheet_id = "1JTVA-9NuBHW1wWtJ4uP118Xerr3utgMlAx_NSl_fCv8"

def run_adaptive_model(df:pd.DataFrame, locationvar:str, CI:float, filepath:Path) -> None:
    '''
    Runs adaptive control model of Rt and smoothed case counts based on what is currently in the 
    gamma_prior module. Takes in dataframe of cases and saves to csv a dataframe of results.
    '''
    # Initialize results df
    res_full = pd.DataFrame(columns=[locationvar,'date'])
    res_full.loc[:,'date'] = pd.to_datetime(res_full['date'], format='%Y-%m-%d')

    # Null smoother to pass to gamma_prior (since smoothing was already done)
    def null_smoother(data: Sequence[float]):
        return data

    # Loop through each location
    print(f"Estimating Adaptive Rt values for each {locationvar}...")
    for loc in tqdm(df[locationvar].unique()):
                
        # Calculate Rt for that location
        loc_df = df[df[locationvar] == loc].set_index('date')
        (
        dates, RR_pred, RR_CI_upper, RR_CI_lower,
        T_pred, T_CI_upper, T_CI_lower,
        total_cases, new_cases_ts,
        _, anomaly_dates
        ) = gamma_prior(loc_df[loc_df['positive_smooth'] > 0]['positive_smooth'], 
                        CI=CI, smoothing=null_smoother)
        assert(len(dates) == len(RR_pred))
        
        # Save results
        res = pd.DataFrame({locationvar:loc,
                            'date':dates,
                            'RR_pred':RR_pred,
                            'RR_CI_upper':RR_CI_upper,
                            'RR_CI_lower':RR_CI_lower,
                            'T_pred':T_pred,
                            'T_CI_upper':T_CI_upper,
                            'T_CI_lower':T_CI_lower,
                            'new_cases_ts':new_cases_ts,
                            'total_cases':total_cases[2:],
                            'anomaly':dates.isin(set(anomaly_dates))})
        res_full = pd.concat([res_full,res], axis=0)
    
    # Merge results back onto input df and return
    merged_df = df.merge(res_full, how='outer', on=[locationvar,'date'])

    # Parameters for filtering raw df
    kept_columns   = ['date',locationvar,'RR_pred','RR_CI_lower','RR_CI_upper','T_pred',
                      'T_CI_lower','T_CI_upper','new_cases_ts','anomaly']
    merged_df      = merged_df[kept_columns]
    
    # Format date properly and return
    merged_df.loc[:,'date'] = pd.to_datetime(merged_df['date'], format='%Y-%m-%d')

    # Save out result
    merged_df.to_csv(filepath/"adaptive_estimates.csv", index=False)


def run_cori_model(filepath:Path, rexepath:Path) -> None:
    '''
    Runs R script that runs Cori model estimates. Saves results in
    a CSV file.
    '''
    subprocess.call([rexepath/"Rscript.exe", filepath/"cori_model.R"], shell=True)


def sync_sheet(df):

    # Fix missing values and undo date format
    df.fillna('', inplace=True)
    df.loc[:,'date'] = df['date'].astype(str)

    # Write values to sheet 
    print("Writing values to sheet...")
    rtcols = [x for x in df.columns if x.startswith('RR_')]
    values = [list(a) for a in df[["state","date"]+rtcols].values] 
    range_ = "Rt_US_States!A2:N"

    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service  = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().values()\
        .update(
            spreadsheetId=sheet_id, 
            range=range_, 
            valueInputOption='USER_ENTERED', 
            body={"values":values}) \
        .execute()
    print("Response from sheets client:", response)


def estimate_and_sync(state):

    # Folder structures and file names
    root    = Path("/tmp")
    data    = root/"data"
    if not data.exists():
        data.mkdir()

    # Get case data
    df = import_clean_smooth_cases(data, notched_smoothing(window=smoothing_window))
    df = df[df['state'] == state]

    # Run models for adaptive and rt.live old version
    run_adaptive_model(df=df, locationvar='state', CI=CI, filepath=data)
    run_luis_model(df=df, locationvar='state', CI=CI, filepath=data)
    run_rtlive_old_model(df=df, locationvar='state', CI=CI, filepath=data)
    # run_cori_model(filepath=root, rexepath=rexepath) # Have to change R file parameters separately

    # Pull CSVs of results
    adaptive_df    = pd.read_csv(data/"adaptive_estimates.csv")
    luis_df        = pd.read_csv(data/"luis_code_estimates.csv")
    rt_live_new_df = get_new_rt_live_estimates(data)
    rt_live_old_df = pd.read_csv(data/"rtlive_old_estimates.csv")
    # cori_df        = pd.read_csv(data/"cori_estimates.csv")

    # Merge all results together
    merged_df      = adaptive_df
    merged_df      = merged_df.merge(luis_df, how='outer', on=['state','date'])
    merged_df      = merged_df.merge(rt_live_new_df, how='outer', on=['state','date'])
    merged_df      = merged_df.merge(rt_live_old_df, how='outer', on=['state','date'])
    # merged_df      = merged_df.merge(cori_df, how='outer', on=['state','date'])

    # Fix date formatting and save results
    # merged_df.loc[:,'date'] = pd.to_datetime(merged_df['date'], format='%Y-%m-%d')
    merged_df.to_csv(data/"+rt_estimates_comparison.csv", index=False)

    # Upload to Cloud
    bucket = storage.Client().bucket(bucket_name)
    blob   = bucket.blob("data/+rt_estimates_comparison.csv").upload_from_filename(str(data/"+rt_estimates_comparison.csv"), content_type="text/csv")

    # Sync sheet with results
    sync_sheet(merged_df)
