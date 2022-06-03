from torch.hub import load
from pyannote.audio import Pipeline
from pyannote.database.util import load_rttm
import speechbrain as sb
from flying_words.audio import Audio

class Diarization:
    def __init__(self, source: Audio, model='dia_dihard'):
        self.model = model
        self.source = source
        # self.pipeline = load('pyannote/pyannote-audio', self.model)
        self.pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization")
        self.diarization = None


    def make_diarization(self):
        self.diarization = self.pipeline(self.source.filepath)
        # self.diarization = self.pipeline({'audio': self.source})
        return self.diarization


    def diarization_output(self):

        output = []
        for turn, _, speaker in self.diarization.itertracks(yield_label=True):
            output.append(dict(speaker=speaker, turn_start=turn.start, turn_end=turn.end))

        return output
