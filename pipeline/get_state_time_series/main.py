from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from epimargin.etl.covid19india import (data_path, download_data,
                                        get_time_series, load_all_data)
from epimargin.utils import mkdir
from google.cloud import storage

# cloud details 
bucket_name = "daily_pipeline"

def run_download(_):
    run_date = pd.Timestamp.now().strftime("%d-%m-%Y") 
    print(f"Starting download of API files on {run_date}")
    # set up
    root = Path("/tmp")
    data = mkdir(root/"data")

    # determine number of files to download from API documentation
    print("Determing latest API file.")
    r = requests.get("https://api.covid19india.org/")
    latest_csv_id = bs(r.text)\
        .find(text = "Raw Data")\
        .findNext("table")\
        .findChildren("a")[-1].text\
        .split("/")[-1]\
            .replace("raw_data", "")\
            .replace(".csv",     "")
    latest_csv_number = int(latest_csv_id)


    print(f"Downloading files up until raw_data{latest_csv_id}.csv.")
    paths = { 
        "v3": [data_path(i) for i in (1, 2)],
        "v4": [data_path(i) for i in range(3, 1 + latest_csv_number)]
    }

    for path in paths["v3"] + paths["v4"] :
        print(f"Downloading {path}.")
        download_data(data, path)

    print("Assembling case time series from case reports.")
    load_all_data(
        v3_paths = [data/path for path in paths['v3']], 
        v4_paths = [data/path for path in paths['v4']]
    )\
    .pipe(lambda _: get_time_series(_, ["detected_state", "detected_district"]))\
    .to_csv(data/"state_case_timeseries.csv")
        

    print("Uploading time series to storage bucket.")
    storage.Client()\
        .bucket(bucket_name)\
        .blob("pipeline/raw/case_time_series.csv")\
        .upload_from_filename(
            str(data/"state_case_timeseries.csv"), 
            content_type = "text/csv")

    
