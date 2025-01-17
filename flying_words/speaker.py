import pandas as pd
import string
import random
from flying_words.audio import Audio
from flying_words.google_clients import StorageClient, BigQueryClient
from pydub import AudioSegment

class Speaker:
    ''' inputs needed :
    - diarization_df : dataframe from diarization Class
    - known_ids : list of speakers ID we know
    - unknown_id : ID of target speaker
    - show_start : timestamp of real episod start
    - episod_ID : ID of the episod
    '''

    def __init__(self):
        self.audio_speakers = []
        self.speakers_mapping = {}
        self.otherIDs_mapping = {}
        self.speaker_ids = []
        self.other_IDs = []
        self.other_speakers = []
        self.ep_result = {}
        self.others_count = None
        self.unknown_dicts = []

    def get_unknown_info(self, diarization_df: pd.DataFrame, merged_diffusion_info):
        '''Provide information (timestamp, extract length & label) to extract audio sample of unknown speaker(s).'''

        known_ids = merged_diffusion_info['known_ids']
        unknown_id = merged_diffusion_info['unknown_id']

        # clear known_ids list in case of no sample (empty list)
        if known_ids:
            known_ids = list(known_ids)
        else:
            known_ids = []
        # get list of segmented speakers (from diarization) and list of speakers from input
        self.audio_speakers = diarization_df['name_id'].unique().tolist()
        self.speaker_ids = known_ids + [unknown_id]

        # count difference between nb of identified speakers by diarization and provided list of speakers
        self.others_count = len(self.audio_speakers) - len(known_ids)
        self.other_IDs = []
        # get unknown speakers IDs, if only 1 speaker to label, no need to identify other speakers
        if self.others_count == 1:
            self.other_speakers = []
        else:
            self.other_speakers = self.audio_speakers[-self.others_count:]

        # create dictionary to match segmented speakers & speaker list
        self.speakers_mapping = dict(zip(self.speaker_ids, self.audio_speakers))

        # When we have 1 speaker not identified by diarization, and we have his ID
        if self.others_count == 1 :

            # get all segments of unknown speaker in descending order by length
            unkown_speaker_segments = diarization_df[diarization_df["name_id"] == self.speakers_mapping[unknown_id]].sort_values(['segment_length'],ascending=False)
            length_unknown = unkown_speaker_segments.iloc[0, unkown_speaker_segments.columns.get_loc('segment_length')]

            # based on segment length (< or > 60 sec), identify 1 or more segments
            self.unknown_dicts = []
            if length_unknown > 60 :
                length_unknown = 60
                timestamp_unknown = unkown_speaker_segments.iloc[0, unkown_speaker_segments.columns.get_loc('start')]
                dict_id = [{'unknown_id' :unknown_id, 'timestamp' : timestamp_unknown, 'length' : length_unknown }]
                self.unknown_dicts.extend(dict_id)
            else :
                total_length = 0
                tracker = 0
                while total_length < 60 :
                    timestamp_unknown = unkown_speaker_segments.iloc[tracker, unkown_speaker_segments.columns.get_loc('start')]
                    length_unknown = unkown_speaker_segments.iloc[tracker, unkown_speaker_segments.columns.get_loc('segment_length')]
                    dict_id = [{'unknown_id' :unknown_id, 'timestamp' : timestamp_unknown, 'length' : length_unknown }]
                    self.unknown_dicts.extend(dict_id)
                    total_length += unkown_speaker_segments.iloc[tracker, unkown_speaker_segments.columns.get_loc('segment_length')]
                    tracker += 1
                    # comment gérer si tracker > nb de ligne du df ?

            # When we have more than 1 speaker not identified by diarization, then we consider all "new" speakers as "unknown" (even the one with label)

        else :

            # get longest segment
            self.unknown_dicts = []
            for id in self.other_speakers :
                id_speaker_segments = diarization_df[diarization_df["name_id"] == id].sort_values(['segment_length'],ascending=False)
                if id_speaker_segments.iloc[0, id_speaker_segments.columns.get_loc('segment_length')] > 60: # only for speakers with segments longer than 60 seconds
                    length_unknown = 60
                    timestamp_unknown = id_speaker_segments.iloc[0, id_speaker_segments.columns.get_loc('start')]
                    other_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 8))  #generate random ID for unknown speakers
                    dict_id = [{'unknown_id' :other_id, 'timestamp' : timestamp_unknown, 'length' : length_unknown }]
                    self.unknown_dicts.extend(dict_id)
                    self.other_IDs.append(other_id)
                else :
                    total_length = 0
                    tracker = 0
                    other_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 8))  #generate random ID for unknown speakers, same ID for each segment
                    while total_length < 60 :
                        timestamp_unknown = id_speaker_segments.iloc[tracker, id_speaker_segments.columns.get_loc('start')]
                        length_unknown = id_speaker_segments.iloc[tracker, id_speaker_segments.columns.get_loc('segment_length')]
                        dict_id = [{'unknown_id' :other_id, 'timestamp' : timestamp_unknown, 'length' : length_unknown }]
                        total_length += id_speaker_segments.iloc[tracker, id_speaker_segments.columns.get_loc('segment_length')]
                        tracker += 1
                        if tracker < id_speaker_segments.shape[0]:
                            self.unknown_dicts.extend(dict_id)
                        else:
                            break
                    self.other_IDs.append(other_id)

        # create dictionary to match segmented speakers & speaker list
        self.otherIDs_mapping = dict(zip(self.other_IDs, self.other_speakers))


    def get_retreated_dataframe(self, diarization_df, merged_diffusion_info):

        ''' rename speakers and provide dataframe with for each segment,
        speaker name, time stamp start & end, length, epidod ID and speaker status '''

        episod_id = merged_diffusion_info['episod_id']
        unknown_id = merged_diffusion_info['unknown_id']
        show_start = merged_diffusion_info['show_start']

        # rename speakers
        if not self.other_speakers:
            diarization_df['name_id'].replace(self.speakers_mapping[unknown_id],unknown_id,inplace = True) # replace the target name, if only 1 unknown (ie. other_speakers list is empty)
        for i in range(len(self.speaker_ids)-1):
            diarization_df['name_id'].replace(self.speakers_mapping[self.speaker_ids[i]],self.speaker_ids[i],inplace = True) # replace the known speakers
        for i in range(len(self.other_speakers)):
            diarization_df['name_id'].replace(self.otherIDs_mapping[self.other_IDs[i]],self.other_IDs[i],inplace = True) # replace the other speakers

        # create retreated DataFrame + drop segments corresponding to voice extracts
        df_rtrt = diarization_df.drop(diarization_df[diarization_df.start < show_start].index)
        # set timestamps to 'real' episod start (withouth voice extracts)
        df_rtrt['start'] = df_rtrt['start'].map(lambda x : x - show_start)
        df_rtrt['end'] = df_rtrt['end'].map(lambda x : x - show_start)

        # add a column with speaker status : known (voice sample with lable in library), unknown (voice sample wihtout label)
        df_rtrt['speaker_status'] = df_rtrt['name_id'].copy()
        df_rtrt['speaker_status'] = df_rtrt['speaker_status'].apply(lambda x : 'known' if x in self.speaker_ids else "unknown")

        # add a column with episod ID
        df_rtrt['episod_id'] = episod_id

        # re-order columns
        column_names = ["episod_id", "name_id", "speaker_status", "start", "end", "segment_length"]
        df_rtrt = df_rtrt.reindex(columns=column_names)

        return df_rtrt


    def upload_samples_tables(self, audio_file : Audio, gsClient : StorageClient, big_query : BigQueryClient, bucket_name, sample_dataset):

        blob_folderpath = 'personnality_sample'

        df_unknown_dict = pd.DataFrame(self.unknown_dicts)
        dict_speakers = list(df_unknown_dict.unknown_id.unique())
        for i, speaker in enumerate(dict_speakers) :
            check_one_row = df_unknown_dict[["unknown_id"]].value_counts()==1
            if check_one_row[speaker] and df_unknown_dict[df_unknown_dict['unknown_id']==speaker]['length'].max() >= 60:
                extract_sample = audio_file.export_sample(start=df_unknown_dict[df_unknown_dict['unknown_id']==speaker]['timestamp'].max(), length=df_unknown_dict[df_unknown_dict['unknown_id']==speaker]['length'].max(), label=speaker)
                blob = gsClient.upload_blob(input_path = extract_sample.filepath, bucket_name=bucket_name, blob_folderpath=blob_folderpath)
                self.unknown_dicts[i]['gs_mp3_sample'] = f"gs://{bucket_name}/{sample_dataset}/{blob.name.split('/')[-1]}"
            elif not check_one_row[speaker]:
                df_speaker = df_unknown_dict[df_unknown_dict['unknown_id']==speaker]
                extract_new_sample = AudioSegment.empty()
                for i in range(df_speaker.shape[0]):
                    extract_new_sample = extract_new_sample + audio_file.export_sample(start=df_speaker.iloc[i]['timestamp'], length=df_speaker.iloc[i]['length'], label=speaker).segment
                    blob = gsClient.upload_blob(input_path = extract_new_sample.filepath, bucket_name=bucket_name, blob_folderpath=blob_folderpath)
                    self.unknown_dicts[i]['gs_mp3_sample'] = f"gs://{bucket_name}/{sample_dataset}/{blob.name.split('/')[-1]}"


        samples_df = pd.DataFrame(data=self.unknown_dicts)
        samples_df = samples_df.drop(columns=["timestamp", "length"])
        samples_df.rename(columns = {'unknown_id':'name_id'}, inplace = True)
        samples_df.dropna(inplace=True)

        if self.others_count == 1:
            big_query.append_row_to_table(dataset='flying_words',  input_df = samples_df, dest_table='sample_personnality_library')
        else :
            big_query.append_row_to_table(dataset='flying_words',  input_df = samples_df, dest_table='unknown_sample_personnality_library')
