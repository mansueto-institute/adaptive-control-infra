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
    # set up
    root = Path("/tmp")
    data = mkdir(root/"data")
    run_date = pd.Timestamp.now().strftime("%d-%m-%Y") 
    print(f"Starting download of vaccination data files on {run_date}")

    download_data(data, "vaccine_doses_statewise.csv")

    print("Uploading vaccination data to storage bucket.")
    storage.Client()\
        .bucket(bucket_name)\
        .blob("pipeline/raw/vaccine_doses_statewise.csv")\
        .upload_from_filename(
            str(data/"vaccine_doses_statewise.csv"), 
            content_type = "text/csv")

    return 'OK!'
