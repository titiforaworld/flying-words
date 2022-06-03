import os
from audio import Audio
from diarization import Diarization
from google_clients import StorageClient

#sudo apt install ffmpeg

print()

# Set raw_data folder path
main_path = os.path.abspath(__file__)
main_dir = os.path.dirname(main_path)
data_raw_path = os.path.join(main_dir, '..', 'raw_data')

# Retrieve all mp3 raw_files
mp3_filepaths = []
for file in os.listdir(data_raw_path):
    if os.path.splitext(file)[1] == '.mp3':
        mp3_filepaths.append(os.path.join(data_raw_path, file))

# Convert all .mp3 in raw_data to .wav
for mp3_filepath in mp3_filepaths:
    audio = Audio(mp3_filepath)
    audio.export_conversion('wav')
    audio.export_sample(start=0, length=600)

# Variables for GCP
project_name = 'intense-elysium-346915'
bucket_name = 'le-wagon-project-75667-antoine'
credential_path = '/home/clement/code/titiforaworld/gcp/intense-elysium-346915-f2127c89e62b.json'

# Instanciate google client
gsClient = StorageClient(project_name, credential_path)

# Get blobs from bucket_name
blobs = gsClient.get_bucket_blobs(bucket_name)

# Download blob
blob_uri = 'gs://le-wagon-project-75667-antoine/data/16119-17.04.2022-ITEMA_22998007-2022C6119S0107-21.mp3'
output_path = os.path.join(data_raw_path, os.path.basename(blob_uri))
gsClient.download_blob(blob_uri, output_path)

# # Diarization
# test_audio = Audio('/home/clement/code/titiforaworld/flying_words/raw_data/samples/16119-17.04.2022-ITEMA_22998007-2022C6119S0107-21_sample_0_600_unknown.wav')
# diarization = Diarization(source=test_audio)
# diarization.make_diarization()
# print(diarization.diarization_output())
