from google.cloud import storage
from google.oauth2 import service_account

class GoogleClient:
    """A class for GCP Management."""

    def __init__(self, project: str, credentials: str):
        """Constructor for GoogleClient class."""

        self.credentials = service_account.Credentials.from_service_account_file(credentials)
        self.client = storage.Client(project, self.credentials)


    def get_bucket_blobs(self, bucket_name: str):
        """Get list of blobs from a given bucket."""

        bucket = self.client.bucket(bucket_name)

        return list(self.client.list_blobs(bucket_name))
