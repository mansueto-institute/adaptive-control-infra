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
        "v4": [data_path(i) for i in (3, 4, 5, 6, 7, 8, 9, 10, 11, 12)]
    }

    for target in paths['v3'] + paths['v4']:
        download_data(tmp, target)

    df_new = load_all_data(
        v3_paths = [tmp/filepath for filepath in paths['v3']], 
        v4_paths = [tmp/filepath for filepath in paths['v4']]
    )

    print("calculating diff")
    df_new["rowhash"] = df_new.apply(lambda x: hash(tuple(x)), axis = 1)
    df_new["report_date"] = run_date 

    print("uploading diff")
    pd.concat([df_old, df_new[~df_new.rowhash.isin(df_old.rowhash)]]).to_csv(filename)
    response = storage.Client()\
        .bucket(bucket_name)\
        .blob(blob_name)\
        .upload_from_filename(filename, content_type = "text/csv")
    print(response)

    print("done")
