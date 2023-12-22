# Imports the Google Cloud client library
from google.cloud import storage

from google.oauth2 import service_account

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "google-cloud.json"


storage_client = storage.Client.from_service_account_json(key_path)

# Instantiates a client
# storage_client = storage.Client()

# The name for the new bucket
bucket_name = "it4043e-it5384"

for i in storage_client.list_blobs(bucket_name):
    print(i)
