import os
from flying_words.audio import Audio, merge_diffusion_with_samples
from flying_words.diarization import Diarization
from flying_words.google_clients import BigQueryClient, StorageClient
from flying_words.transcription import Transcription
from flying_words.speaker import Speaker

import pandas as pd
import json


# # Set raw_data folder path
# main_path = os.path.abspath(__file__)
# main_dir = os.path.dirname(main_path)
# data_raw_path = os.path.join(main_dir, 'raw_data')

# # Variables for GCP
# project_name = 'intense-elysium-346915'
# bucket_name = 'le-wagon-project-75667-antoine'
# credential_path = '/home/clement/code/titiforaworld/gcp/intense-elysium-346915-f2127c89e62b.json'

# # Instanciate google clients
# gsClient = StorageClient(project = project_name, credentials = credential_path)
# big_query = BigQueryClient(project = project_name, credentials = credential_path)


# # Get episod & samples from BQ
# job = big_query.get_table(dataset='flying_words', table_name='view_target_output')

# # get merged audio file
# merge_audio_info = merge_diffusion_with_samples(job,gsClient)
# audio_wav = merge_audio_info['merged_audio']

# # Diarization
# diarization_audiowav = Diarization(audio_wav)
# diarization_audiowav.make_diarization(min_duration_off = 1.0)
# segmentation_df = diarization_audiowav.get_diarization_df()

# # get needed inputs
# known_ids = merge_audio_info['known_ids']
# unknown_id = merge_audio_info['unknown_id']
# episod_id = merge_audio_info['episode_id']
# start_ep = merge_audio_info['show_start']

# # get needed information for sample extract
# speaker = Speaker()
# ep_info = speaker.get_unknown_info(segmentation_df, known_ids, unknown_id)

# # get retreated dataframe
# retreated_df = speaker.retreated_dataframe(segmentation_df, episod_id, unknown_id, start_ep)

# # upload retreated dataframe to Big Query
# big_query.append_row_to_table(dataset='flying_words',  input_df = retreated_df, dest_table='segmentation')

# # upload speaker samples to Could Storage
# sample_dataset = "personnality_sample"
# speaker.upload_samples_tables(audio_file=merge_audio_info['show_start'], gsClient=gsClient, big_query=big_query, bucket_name=bucket_name, sample_dataset=sample_dataset)

# # transcription = Transcription(sample).make_transcription()
# #

import os
from flying_words.flow import build_flow

gcp_credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
gcp_project = os.getenv('GCP_PROJECT')
gcp_bucket= os.getenv('GCP_BUCKET')

env_vars = dict(gcp_credentials_path=gcp_credentials_path,
            gcp_project=gcp_project,
            gcp_bucket=gcp_bucket)

flow = build_flow(env_vars)

flow.run()
