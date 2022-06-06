import os
from flying_words.audio import Audio
from pydub import AudioSegment
from flying_words.google_clients import StorageClient

# test modif

class Compilation:
    '''
    Given links of samples of the known speakers (pd.Series) and the link of the targeted episode,
    export the audio file of the concatenation of the samples and the episode,
    and return the start of the show and the list of the known speakers.
    '''

    def __init__(self, query_job):
        self.episode_mp3_link = query_job['episode_lien_mp3_google_storage'][0]
        self.episode_id = query_job['episode_id'][0]
        self.known_ids = query_job['name_id_known_guest'][:]
        self.unknown_id = query_job['personnality_to_sample_name_id'][0]
        self.sample_segments = query_job['gs_mp3_sample'][:]
        self.query_job = query_job
        self.start_show = None
        self.path_to_episode = None
        self.path_to_samples = None

    def get_compiled_audio(self, gsClient:StorageClient):

        self.path_to_episode = '/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/episodes'
        self.path_to_samples = '/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/samples'

        # # Download blob
        blob_uri = self.episode_mp3_link
        output_path = os.path.join('/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/episodes', os.path.basename(blob_uri))
        dld_episode = gsClient.download_blob(blob_uri, output_path)

        dld_samples = []

        for blob in self.sample_segments:
            blob_uri = blob
            output_path = os.path.join('/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/samples', os.path.basename(blob_uri))
            dld_samples.append(gsClient.download_blob(blob_uri, output_path))

        episode_name = (dld_episode.name).split("/")[1]
        samples_names = []

        for s in range(len(self.sample_segments)):
            samples_names.append((dld_samples[s].name).split('/')[1])

        path_episode = os.path.join(self.path_to_episode, episode_name)

        path_samples = []
        for n in range(len(samples_names)):
            path_samples.append(os.path.join(self.path_to_samples, samples_names[n]))

        Audio(path_episode).load_source()

        audio_samples_mp3 = []

        for i in range(len(path_samples)):
            audio_samples_mp3.append(Audio(path_samples[i]).load_source())

        # Combination
        path_episode = path_episode
        paths_to_combine = []
        for  i  in range(0, len(audio_samples_mp3)) :
            paths_to_combine.append(f"{self.path_to_samples}/{samples_names[i]}")

        combined = AudioSegment.empty()

        for  i  in range(0, len(paths_to_combine)) :
            combined = combined + AudioSegment.from_file(paths_to_combine[i], format='mp3')

        audio_full = combined + AudioSegment.from_file(path_episode, format='mp3')

        self.start_show = combined.duration_seconds

        audio_full.export(f"/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/outputs/{episode_name}.wav", format="wav")

        return Audio(f"/content/drive/MyDrive/projetWagon/data/compilation_samples_episode/outputs/{episode_name}.wav")


    def get_known_ids(self):
        return list(self.known_ids)
