import os
from flying_words.google_clients import StorageClient
from flying_words.utils import time_it

import pydub
from pydub import AudioSegment

from copy import copy

from google.cloud import storage



class Audio:
    """A class for audio processing."""

    def __init__(self, source: str or StorageClient):
        """Constructor for Audio class."""

        # if isinstance(source, GoogleClient):
        #     source.

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
