import os
import azure.cognitiveservices.speech as speechsdk
from flying_words.audio import Audio
import json
import time
import wave
import pandas as pd

from flying_words.google_clients import BigQueryClient, StorageClient

class Transcription:
    """A class for transcription."""

    def __init__(self, source: Audio, episode_id: str):
        """Constructor for the class Transcription."""

        self.source = source
        self.episode_id = episode_id
        self.result = None


    def make_transcription(self, azure_token):
        """Process diarization on audio-souce."""

        speech_config = speechsdk.SpeechConfig(subscription=azure_token,
                                               region="francecentral",
                                               speech_recognition_language = 'fr-FR')

        speech_config.request_word_level_timestamps()

        speech_config.output_format = speechsdk.OutputFormat(1)

        audio_config = speechsdk.audio.AudioConfig(filename=self.source.filepath)

        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config,
                                                       audio_config=audio_config)

        done = False

        # Service callback for recognition text
        transcript_display = []
        confidence_list = []
        words = []
        def parse_azure_result(evt):
            response = json.loads(evt.result.json)
            transcript_display.append(response['DisplayText'])
            confidence_list_temp = [item.get('Confidence') for item in response['NBest']]
            max_confidence_index = confidence_list_temp.index(max(confidence_list_temp))
            confidence_list.append(response['NBest'][max_confidence_index]['Confidence'])
            words.extend(response['NBest'][max_confidence_index]['Words'])
            print(evt.result.json)

        # Service callback that stops continuous recognition upon receiving an event `evt`
        def stop_cb(evt):
            print('CLOSING on {}'.format(evt))
            speech_recognizer.stop_continuous_recognition()
            nonlocal done
            done = True

        # Connect callbacks to the events fired by the speech recognizer
        # speech_recognizer.recognizing.connect(lambda evt: print('RECOGNIZING: {}'.format(evt)))
        speech_recognizer.recognized.connect(parse_azure_result)
        speech_recognizer.session_started.connect(lambda evt: print('SESSION STARTED: {}'.format(evt)))
        speech_recognizer.session_stopped.connect(lambda evt: print('SESSION STOPPED {}'.format(evt)))
        speech_recognizer.canceled.connect(lambda evt: print('CANCELED {}'.format(evt)))
        # stop continuous recognition on either session stopped or canceled events
        speech_recognizer.session_stopped.connect(stop_cb)
        speech_recognizer.canceled.connect(stop_cb)

        # Start continuous speech recognition
        print("Initiating speech to text")
        speech_recognizer.start_continuous_recognition()
        while not done:
            time.sleep(.5)

        self.result = dict(transcript_display=transcript_display,
                           confidence_list=confidence_list,
                           words=words)

        return self.result


    def upload_to_gcp(self, gsClient: StorageClient, bucket_name, bqClient: BigQueryClient):

        dataset = 'flying_words'
        table = 'episode'

        # Create transcript folder if doesn't exist
        transcript_folder_path = os.path.join('raw_data', 'transcripts')
        os.makedirs(transcript_folder_path, exist_ok=True)

        transcript_path = os.path.join(transcript_folder_path, f'transcript_{self.episode_id}.txt')
        with open(transcript_path, 'w', encoding='utf8') as file:
            file.write(' '.join(self.result['transcript_display']))

        transcript_dict_path = os.path.join(transcript_folder_path, f'transcript_dict_{self.episode_id}.txt')
        with open(transcript_dict_path, 'w', encoding='utf8') as file:
            file.write(str(self.result['words']))

        transcript_blob = gsClient.upload_blob(transcript_path, bucket_name, 'transcript')
        transcript_dict_blob = gsClient.upload_blob(transcript_dict_path, bucket_name, 'transcript_dict')

        transcript_blob_uri = f'gs://{bucket_name}/{transcript_blob.name}'
        transcript_dict_blob_uri = f'gs://{bucket_name}/{transcript_dict_blob.name}'

        bqClient.update_table(dataset, table,
                              'id', self.episode_id,
                              'transcription', transcript_blob_uri)

        bqClient.update_table(dataset, table,
                              'id', self.episode_id,
                              'transcription_dict', transcript_dict_blob_uri)

        return transcript_blob_uri, transcript_dict_blob_uri
