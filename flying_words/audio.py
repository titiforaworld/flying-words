import os
from flying_words.google_clients import StorageClient
from flying_words.utils import time_it

import pydub
from pydub import AudioSegment

from copy import copy

from google.cloud import storage

import pandas as pd



class Audio:
    """A class for audio processing."""

    def __init__(self, source: str):
        """Constructor for Audio class."""

        self.filepath = source
        self.root, self.format = os.path.splitext(self.filepath)

        self.segment = self.load_source()


    def load_source(self):
        """Load audio-source as AudioSegment."""

        if self.format == '.mp3':
            self.segment = AudioSegment.from_mp3(self.filepath)
        elif self.format == '.wav' :
            self.segment = AudioSegment.from_wav(self.filepath)

        self.segment = self.segment.set_channels(1)
        self.segment = self.segment.set_frame_rate(16000)

        return self.segment


    def export_sample(self, start: int, length: int, label: str = 'unknown'):
        """Export a .wav sample from audio-source.

        Samples are kept in self.samples list.
        Inputs are in seconds.
        """

        # Create samples folder if doesn't exist
        sample_folder_path = os.path.join(os.path.dirname(self.filepath), 'samples')
        os.makedirs(sample_folder_path, exist_ok=True)

        # Create output filepath
        sample_filename_suffix = f'_sample_{start}_{length}'
        output_filename = f'{os.path.splitext(os.path.basename(self.filepath))[0]}{sample_filename_suffix}_{label}.wav'
        output_path = os.path.join(sample_folder_path, output_filename)

        # Cancel if file already exists
        if os.path.exists(output_path):
            print(f'{os.path.basename(output_path)}: File already exists. Loading it.')
            return Audio(output_path)

        # load_source
        if not self.segment:
            self.segment = self.load_source()

        # Get start and end timestamp in ms
        t_start = start * 1000
        t_end = t_start + length * 1000

        # Extract sample part
        sample_segment = self.segment[t_start:t_end]

        sample_segment = sample_segment.set_channels(1)
        sample_segment = sample_segment.set_frame_rate(16000)

        # Generate output file
        try:
            sample_segment.export(output_path, format='wav')
            print(f'{os.path.basename(output_path)}: File created')
            print('Sampling Succeeded')
        except Exception as e:
            print(f'Sampling Aborted : {e}')

        return Audio(output_path)


    def export_conversion(self, format: str):
        """Export a conversion of audio-source in a given format."""

        output_path = f'{self.root}.{format}'

        # Cancel if file already exists
        if os.path.exists(output_path):
            print(f'{os.path.basename(output_path)}: File already exists. Loading it.')
            return Audio(output_path)

        # load_source
        if not self.segment:
            self.segment = self.load_source()

        # Configure conversion
        segment_to_convert = copy(self.segment)
        segment_to_convert = segment_to_convert.set_channels(1)
        segment_to_convert = segment_to_convert.set_frame_rate(16000)

        # Conversion
        try:
            segment_to_convert.export(output_path, format=format)
            print(f'{os.path.basename(output_path)}: File created')
            print('Conversion Succeeded')
        except Exception as e:
            print(f'Conversion Aborted : {e}')

        return Audio(output_path)

def merge_diffusion_with_samples(target_response: pd.DataFrame, gsClient:StorageClient):

    diffusion_blob_uri = target_response['episode_lien_mp3_google_storage'][0]
    episode_id = target_response['episode_id'][0]
    known_ids = target_response['name_id_known_guest'][:]
    unknown_id = target_response['personnality_to_sample_name_id'][0]
    sample_blobs_uri = target_response['gs_mp3_sample'][:]

    episode_folder = '/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/episodes' # TODO
    samples_folder = '/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/samples' # TODO
    merge_folder = '/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/merges' # TODO


    # Download diffusion blob
    diffusion_path = os.path.join(episode_folder, os.path.basename(diffusion_blob_uri))
    gsClient.download_blob(diffusion_blob_uri, diffusion_path)
    diffusion_audio = Audio(diffusion_path).export_conversion('wav')

    # Retrieve samples information and download blob
    sample_audios = []

    if sample_blobs_uri.any():
        for sample_blob_uri in sample_blobs_uri:
            sample_path = os.path.join(samples_folder, os.path.basename(sample_blob_uri))
            gsClient.download_blob(sample_blob_uri, sample_path)
            sample_audios.append(Audio(sample_path).export_conversion('wav'))
    else:
        sample_audios = []

    # Samples Merging
    merged_audio = AudioSegment.empty()
    for sample_audio in sample_audios:
        merged_audio += sample_audio

    # Get samples time offset
    show_start = merged_audio.duration_seconds

    # Add diffusion to merged audio
    merged_audio += diffusion_audio

    # Export merged audio
    merge_filename = os.path.splitext(os.path.basename(diffusion_blob_uri))[0]
    merge_path = os.path.join(merge_folder, f'{merge_filename}.wav')
    merged_audio.export(merge_path, format="wav")

    merged_audio_info = dict(episode_id=episode_id,
                                known_ids=known_ids,
                                unknown_id=unknown_id,
                                merged_audio=Audio(merge_path),
                                show_start=show_start,
                                diffusion_audio=diffusion_audio)

    return merged_audio_info
