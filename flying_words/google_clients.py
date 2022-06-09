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

    def get_transcript_df(self, dict_blob_uri: str, text_blob_uri: str, output_path: str):

        self.download_blob(dict_blob_uri, output_path)
        with open(output_path) as f:
            text_dict = f.read()

        text_dict_df = pd.DataFrame(eval(text_dict))
        text_dict_df['Offset'] = text_dict_df['Offset']/ 10000000
        text_dict_df['End_word'] = text_dict_df['Offset'] + text_dict_df['Duration'] / 10000000

        self.download_blob(text_blob_uri, output_path)

        with open(output_path) as f:
            text_file = f.read()

            text_split = text_file.split(' ')

        text_file_df = pd.DataFrame(text_split, columns=["Word"])
        nb_row_dict=text_dict_df.shape[0]
        nb_row_text=text_file_df.shape[0]
        if nb_row_text>=nb_row_dict:
            text_file_df["Offset"]=text_dict_df["Offset"]
            text_file_df["Duration"]=text_dict_df["Duration"]
            text_file_df["Confidence"]=text_dict_df["Confidence"]
            text_file_df["End_word"]=text_dict_df["End_word"]
            return text_file_df
        elif  nb_row_text<nb_row_dict:
            text_dict_df["Word"][:nb_row_text] =text_file_df["Word"]
            text_dict_df["Word"][nb_row_text:] =""
            return text_dict_df




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

        job = self.client.query(query)

        job.result()

        assert job.num_dml_affected_rows is not None

        return job.num_dml_affected_rows


    def episode_speaking_time_df(self, episode_id:str):
        ###filter on episode_id
        segmentation = self.get_table(dataset='flying_words', table_name='segmentation')
        segmentation_filter_episode = segmentation[segmentation["episod_id"] == episode_id]
        segmentation_filter_episode = segmentation_filter_episode.sort_values(by ="start")
        j=0
        ###create range_speaker column
        segmentation_filter_episode["range_speaker"]=0.0

        ###loop over
        for i in range(len(segmentation_filter_episode["episod_id"])):
            if i!=0 :
                if segmentation_filter_episode["name_id"].iloc[i] != segmentation_filter_episode["name_id"].iloc[i-1]:
                    j=j+1

            segmentation_filter_episode["range_speaker"].iloc[i]=j


        ordered_speaking_time = segmentation_filter_episode.groupby(["episod_id","range_speaker","name_id"],as_index=False).agg({"start":'min',"end":"max","segment_length":'sum' } )

        return ordered_speaking_time


    def words_diarization_info_merger(self, text_file_df :pd.DataFrame, episode_id:str, bqClient):
        """
        text_file_df is taken from the get_transcript_df function above.
        """
        speak_time = self.episode_speaking_time_df(episode_id).sort_values('start')

        # Create the text_file_df
        text_file_df["name_id"] ="to_be_filled"
        text_file_df['range_speaker'] = "to_be_filled"

        j=0
        for i in range(text_file_df.shape[0]):
            if text_file_df["End_word"].iloc[i] < speak_time["end"].loc[j]:
                text_file_df["name_id"].iloc[i] = speak_time["name_id"].loc[j]
                text_file_df["range_speaker"].iloc[i] = speak_time["range_speaker"].loc[j].astype(int)

        else :
            text_file_df["name_id"].iloc[i]=speak_time["name_id"].loc[j+1]
            text_file_df["range_speaker"].iloc[i] = speak_time["range_speaker"].loc[j+1].astype(int)
            j=j+1

        # Create the text_list
        speaking_time_by_speaker_df = text_file_df.groupby('range_speaker', as_index=False).idxmax()

        text_list=[]
        for j in range(speaking_time_by_speaker_df.shape[0]):
            if j==0 :
                text_list.append(" ".join([ text_file_df["Word"].iloc[i] for i in range(speaking_time_by_speaker_df["End_word"].iloc[j])]))
            else :
                text_list.append(" ".join([ text_file_df["Word"].iloc[i] for i in range(speaking_time_by_speaker_df["End_word"].iloc[j-1], speaking_time_by_speaker_df["End_word"].iloc[j])]))

        # Add the transcript to the speak_time dataframe
        speak_time['transcript'] = pd.Series(text_list)

        return speak_time
