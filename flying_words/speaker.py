import pandas as pd


class Speaker:
    def __init__(self):
        pass

    def get_unknown_info(diarization_df: pd.DataFrame, known_ids, unknown_id):
        '''Provide information to extract audio sample of unknown speaker.'''

        speaker_ids = known_ids.append(unknown_id)

        # create dictionary to match audio speakers & speaker list
        audio_speakers = diarization_df['speaker'].unique().tolist()

        speakers_mapping = dict(zip(speaker_ids, audio_speakers))

        # identify longest segment of all speakers
        max_table = diarization_df.loc[diarization_df.groupby(["speaker"])["segment_length"].idxmax()]
        max_table = max_table.set_index('speaker')
        # get timestamp & length of segment for unknown speaker
        timestamp_unknown = max_table.loc[speakers_mapping[unknown_id], 'start']
        length_unknown = max_table.loc[speakers_mapping[unknown_id], 'segment_length']

        return timestamp_unknown, length_unknown, unknown_id
