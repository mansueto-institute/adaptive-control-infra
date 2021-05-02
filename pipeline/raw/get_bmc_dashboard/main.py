import datetime

import requests
from google.cloud import storage

bucket_name = "daily_pipeline"
URL = "https://stopcoronavirus.mcgm.gov.in/assets/docs/Dashboard.pdf"

def run_download(_):
    date = datetime.datetime.now().strftime("%m_%d_%Y")
    filename = f"bmc_dashboard_{date}.pdf"
    print(f"Downloading BMC dashboard for date {date}.")

    response = requests.get(URL, verify = False)
    with open(f"/tmp/{filename}", "wb") as dst:
        dst.write(response.content)
    
    print("Download complete; uploading to Cloud Storage.")

    storage.Client()\
        .bucket(bucket_name)\
        .blob(f"pipeline/raw/bmc/{filename}")\
        .upload_from_filename(f"/tmp/{filename}", content_type = "application/pdf")
    return 'OK!'
