import os
import azure.cognitiveservices.speech as speechsdk
from flying_words.audio import Audio
import json
import time
import wave

class Transcription:
    """A class for transcription."""

    def __init__(self, source: Audio):
        """Constructor for the class Transcription."""

        self.source = source


    def make_transcription(self):
        """Process diarization on audio-souce."""

        speech_config = speechsdk.SpeechConfig(subscription=os.getenv("AZURE_TOKEN"),
                                               region="francecentral",
                                               speech_recognition_language = 'fr-FR')

        speech_config.request_word_level_timestamps()

        audio_config = speechsdk.audio.AudioConfig(filename=self.source.filepath)

        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config,
                                                       audio_config=audio_config)

        done = False
        result = None

        def stop_cb(evt):
            """Callback that stops continuous recognition upon receiving an event."""
            speech_recognizer.stop_continuous_recognition()
            nonlocal done
            done = True

        def get_result_cb(evt):
            """Callback to get transcription result."""
            nonlocal result
            result = evt

        # Connect callbacks to the events fired by the speech recognizer
        speech_recognizer.recognized.connect(lambda evt: print(f'Transcription finished'))
        speech_recognizer.session_started.connect(lambda evt: print(f'Transcription started'))
        speech_recognizer.session_stopped.connect(lambda evt: print(f'Transcription stopped'))
        speech_recognizer.canceled.connect(lambda evt: print(f'Transcription canceled : {evt.result.cancellation_details}'))
        # stop continuous recognition on either session stopped or canceled events
        speech_recognizer.session_stopped.connect(stop_cb)
        speech_recognizer.canceled.connect(stop_cb)
        speech_recognizer.recognized.connect(get_result_cb)

        # Start continuous speech recognition
        speech_recognizer.start_continuous_recognition()
        while not done:
            time.sleep(.5)

        return result
