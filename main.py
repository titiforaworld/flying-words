import os
from flying_words.audio import Audio, merge_diffusion_with_samples
from flying_words.diarization import Diarization
from flying_words.google_clients import BigQueryClient, StorageClient
from flying_words.transcription import Transcription
from flying_words.speaker import Speaker

import pandas as pd


# Set raw_data folder path
main_path = os.path.abspath(__file__)
main_dir = os.path.dirname(main_path)
data_raw_path = os.path.join(main_dir, 'raw_data')

# # Retrieve all mp3 raw_files
# mp3_filepaths = []
# for file in os.listdir(data_raw_path):
#     if os.path.splitext(file)[1] == '.mp3':
#         mp3_filepaths.append(os.path.join(data_raw_path, file))

# # Convert all .mp3 in raw_data to .wav
# for mp3_filepath in mp3_filepaths:
#     audio = Audio(mp3_filepath)
#     audio.export_conversion('wav')
#     audio.export_sample(start=0, length=600)

# Variables for GCP
project_name = 'intense-elysium-346915'
bucket_name = 'le-wagon-project-75667-antoine'
credential_path = '/home/clement/code/titiforaworld/gcp/intense-elysium-346915-f2127c89e62b.json'


# # Instanciate google clients
gsClient = StorageClient(project = project_name, credentials = credential_path)
big_query = BigQueryClient(project = project_name, credentials = credential_path)

# # Get blobs from bucket_name
# blobs = gsClient.get_bucket_blobs(bucket_name)

# # Download blob
# blob_uri = 'gs://le-wagon-project-75667-antoine/data/16119-17.04.2022-ITEMA_22998007-2022C6119S0107-21.mp3'
# output_path = os.path.join(data_raw_path, os.path.basename(blob_uri))
# gsClient.download_blob(blob_uri, output_path)

# # Upload blob
# input_path = '/home/clement/code/titiforaworld/flying_words/raw_data/001_test'
# gsClient.upload_blob(input_path, bucket_name, 'data/')

# get episod & samples from BQ
job = big_query.get_table(dataset='flying_words', table_name='view_target_output')

# get merged audio file
merge_audio_info = merge_diffusion_with_samples(job,gsClient)
audio_wav = merge_audio_info['merged_audio']

# Diarization
diarization_audiowav = Diarization(audio_wav)
diarization_audiowav.make_diarization(min_duration_off = 1.0)
segmentation_df = diarization_audiowav.get_diarization_df()

# test_audio = Audio('/home/clement/code/titiforaworld/flying_words/raw_data/16119-17.04.2022-ITEMA_22998007-2022C6119S0107-21.mp3')
# sample = test_audio.export_sample(20, 20)

# upload speaker samples
# get needed inputs
known_ids = merge_audio_info['known_ids']
unknown_id = merge_audio_info['unknown_id']
episod_id = merge_audio_info['episode_id']
start_ep = merge_audio_info['show_start']
# get needed information for sample extract
speaker = Speaker()
ep_info = speaker.get_unknown_info(segmentation_df, known_ids, unknown_id)
# get retreated dataframe
retreated_df = speaker.retreated_dataframe(segmentation_df, episod_id, unknown_id, start_ep)

# upload retreated dataframe to Big Query
big_query.append_row_to_table(dataset='flying_words',  input_df = retreated_df, dest_table='segmentation')

# upload speaker samples to Could Storage
sample_dataset = "personnality_sample"
speaker.upload_samples_tables(audio_file=merge_audio_info['show_start'], gsClient=gsClient, big_query=big_query, bucket_name=bucket_name, sample_dataset=sample_dataset)

# transcription = Transcription(sample).make_transcription()
