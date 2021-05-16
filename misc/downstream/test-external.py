from google.cloud import storage
from pathlib import Path 

root = Path(__file__).parent

bucket_name = "daily_pipeline"
for blob in storage.Client().list_blobs(bucket_name, prefix = "pipeline/est"):
    filename = Path(blob.name).name
    print(f"{blob.name} -> {filename}")
    # blob.download_to_filename(root / filename)
