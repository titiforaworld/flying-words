import os
import re

from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery

import datetime
import pandas as pd

import db_dtypes

class StorageClient:
    """A class for Google Storage Management."""

    def __init__(self, project: str, credentials: str):
        """Constructor for GoogleClient class."""

        self.credentials = service_account.Credentials.from_service_account_file(credentials)
        self.client = storage.Client(project, self.credentials)
        self.project = project


    def get_bucket_blobs(self, bucket_name: str):
        """Get list of blobs from a given bucket."""

        bucket = self.client.bucket(bucket_name)

        return list(self.client.list_blobs(bucket_name))


    def download_blob(self, blob_uri: str, output_path: str):
        """Downloads a blob from the bucket."""

        blob_uri_pattern = r'gs:\/\/([^\/]*)\/(.*)'
        bucket_name, blob_path = re.search(blob_uri_pattern, blob_uri).groups()

        bucket = self.client.bucket(bucket_name)

        blob = bucket.blob(blob_path)

        blob.download_to_filename(output_path)

        return blob


    def upload_blob(self, input_path: str, bucket_name, blob_folderpath: str):
        """Upload a blob from the bucket."""

        bucket = self.client.bucket(bucket_name)

        blob_path = os.path.join(blob_folderpath, os.path.basename(input_path))

        blob = bucket.blob(blob_path)

        blob.upload_from_filename(input_path)

        return blob


class BigQueryClient:
    """A class for Big Query Management."""

    def __init__(self, project: str, credentials: str):
        """Constructor for GoogleClient class."""

        self.credentials = service_account.Credentials.from_service_account_file(credentials)
        self.client = bigquery.Client(project, self.credentials)
        self.project = project

    def append_row_to_table(self, dataset: str, input_df: pd.DataFrame, dest_table: str):
        '''
        function to import in "intense-elysium-346915" project
        data in a given table ==> episode ou episode_guest
        '''
        table_id = f"{dataset}.{dest_table}"

        job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition="WRITE_APPEND")

        job = self.client.load_table_from_dataframe(input_df,
                                                    table_id,
                                                    job_config=job_config)

        job.result()

        # Make an API request.
        table = self.client.get_table(table_id)

        return (table.num_rows, len(table.schema), table_id)

    def get_table(self, dataset: str, table_name: str):
        '''Get table from Big Query.'''

        query = f'SELECT * FROM `{self.project}.{dataset}.{table_name}`'

        job = self.client.query(query)

        return job.to_dataframe()

    def update_table(self, dataset: str, table_name: str,
                     column_condition: str, value_condition,
                     column: str, new_value):

        query = f"""
            UPDATE `{self.project}.{dataset}.{table_name}`
            SET {column} = '{new_value}'
            WHERE {column_condition} = '{value_condition}'
            """
        print(query)
        job = self.client.query(query)

        job.result()

        assert job.num_dml_affected_rows is not None

        return job.num_dml_affected_rows
