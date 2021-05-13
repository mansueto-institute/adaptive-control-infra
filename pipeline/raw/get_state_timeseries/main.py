from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from epimargin.etl.commons import download_data
from epimargin.etl.covid19india import (data_path, get_time_series,
                                        load_all_data)
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

    # download aggregated CSVs as well
    download_data(data, "states.csv")
    download_data(data, "districts.csv")

    print("Uploading time series to storage bucket.")
    bucket = storage.Client().bucket(bucket_name)
    bucket.blob("pipeline/raw/districts.csv")\
        .upload_from_filename(str(data/"districts.csv"), content_type = "text/csv")
    bucket.blob("pipeline/raw/states.csv")\
        .upload_from_filename(str(data/"states.csv"), content_type = "text/csv")

    return 'OK!'
