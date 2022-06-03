import os
import re

from google.cloud import storage
from google.oauth2 import service_account

class StorageClient:
    """A class for Google Storage Management."""

    def __init__(self, project: str, credentials: str):
        """Constructor for GoogleClient class."""

        self.credentials = service_account.Credentials.from_service_account_file(credentials)
        self.client = storage.Client(project, self.credentials)


    def get_bucket_blobs(self, bucket_name: str):
        """Get list of blobs from a given bucket."""

        bucket = self.client.bucket(bucket_name)

        return list(self.client.list_blobs(bucket_name))


    def download_blob(self, blob_uri: str, output_path: str = os.path.join('..', 'raw_data')):
        """Downloads a blob from the bucket."""

        blob_uri_pattern = r'gs:\/\/([^\/]*)\/(.*)'
        bucket_name, blob_path = re.search(blob_uri_pattern, blob_uri).groups()

        bucket = self.client.bucket(bucket_name)

        blob = bucket.blob(blob_path)

        blob.download_to_filename(output_path)

        return blob
