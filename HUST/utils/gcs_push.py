from google.cloud import storage
import os
from .load_config import load_config
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/google-cloud.json"

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def push():
    cfg = load_config("config/model.yaml")
    bucket_name = cfg['bucket_name']
    source_file_name = cfg["result_paths"].format(datetime.utcnow().date())
    destination_blob_name = cfg["destination_blob_name"].format(source_file_name)

    upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

def delete_folder(bucket_name, folder_name):
    """Deletes a folder in the GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_name)  # List all objects that start with the folder name

    for blob in blobs:
        blob.delete()  # Delete each object

    print(f"Deleted everything in {folder_name} from {bucket_name}")

if __name__ == "__main__":
    pass
# Replace these variables with your details
    # bucket_name = 'your-bucket-name'
    # source_file_name = 'local/path/to/file'
    # destination_blob_name = 'storage-object-name'

    # upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
