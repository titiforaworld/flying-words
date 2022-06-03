from torch.hub import load
from pyannote.audio import Pipeline
from pyannote.database.util import load_rttm
import speechbrain as sb
from flying_words.audio import Audio

class Diarization:
    def __init__(self, source: Audio):
        self.source = source
        self.pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization")
        self.diarization = None


    def make_diarization(self,
                         onset: float = 0.8,
                         offset: float = 0.4,
                         min_duration_on: float = 10.0,
                         min_duration_off: float = 0.0):
        """Process diarization on audio-souce.

        onset=0.6: mark region as active when probability goes above 0.6
        offset=0.4: switch back to inactive when probability goes below 0.4
        min_duration_on=0.0: remove active regions shorter than that many seconds
        min_duration_off=0.0: fill inactive regions shorter than that many seconds
        """

        initial_params = {"onset": onset,
                          "offset": offset,
                          "min_duration_on": min_duration_on,
                          "min_duration_off": min_duration_off}

        self.pipeline.instantiate(initial_params)

        self.diarization = self.pipeline(self.source.filepath)

        return self.diarization


    def diarization_output(self):

        output = []
        for turn, _, speaker in self.diarization.itertracks(yield_label=True):
            output.append(dict(speaker=speaker, turn_start=turn.start, turn_end=turn.end))

        return output
