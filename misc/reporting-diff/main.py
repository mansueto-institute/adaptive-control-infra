from pathlib import Path
import pandas as pd

from adaptive.etl.covid19india import data_path, download_data, load_all_data
from google.cloud import storage

bucket_name = "adaptive-control-daily-pipeline"
blob_name   = "reporting-diff/daily_diff.csv"
filename    = blob_name.replace("reporting-diff", "/tmp")

def reporting_diff(_):
    tmp = Path("/tmp")
    run_date = str(pd.Timestamp.now()).split()[0]
    print(f"run date: {run_date}")

    # download csv from cloud storage
    print("downloading csv")
    storage.Client()\
        .bucket(bucket_name)\
        .blob(blob_name)\
        .download_to_filename(filename)
    
    print("loading csv")
    df_old = pd.read_csv(filename)
    df_old = df_old.drop(columns = [col for col in df_old.columns if col.startswith("Unnamed")])

    print("downloading latest data")
    paths = {
        "v3": [data_path(i) for i in (1, 2)],
        "v4": [data_path(i) for i in range(3, 21)]
    }

    for target in paths['v3'] + paths['v4']:
        try:
            download_data(tmp, target)
        except:
            pass 

    df_new = load_all_data(
        v3_paths = [tmp/filepath for filepath in paths['v3']], 
        v4_paths = [tmp/filepath for filepath in paths['v4'] if (tmp/filepath).exists()]
    )

    print("calculating diff")
    df_new["rowhash"] = df_new[["patient_number", "date_announced", "detected_district", "detected_state","current_status", "status_change_date", "num_cases"]].apply(lambda x: hash(tuple(x)), axis = 1)
    df_new = df_new.drop_duplicates(subset=["rowhash"], keep="first")
    df_new["report_date"] = run_date
    diff = df_new[~df_new.rowhash.isin(df_old.rowhash)]
    num_new_rows = len(diff)

    print(f"uploading diff ({num_new_rows} new rows written)")
    pd.concat([df_old, diff]).to_csv(filename)
    storage.Client()\
        .bucket(bucket_name)\
        .blob(blob_name)\
        .upload_from_filename(filename, content_type = "text/csv")

    print("done")
