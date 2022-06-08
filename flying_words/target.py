from flying_words.google_clients import BigQueryClient, StorageClient
import requests
import os


class Target:
    def __init__(self, bqClient: BigQueryClient):

        self.bqClient = bqClient
        self.table = None
        self.load_table()

    def load_table(self):
        self.table = self.bqClient.get_table('flying_words', 'view_target_output').iloc[0]
        return self.table

    def update_target_diffusion_storage_link(self, gsClient: StorageClient, bucket_name):

        if len(self.table) == 0:
            return None

        dataset = 'flying_words'
        episode_table = 'episode'
        if self.table['episode_lien_mp3_google_storage'] == 'to be filled':
            diffusions_table = self.bqClient.get_table(dataset, episode_table)
            target_diffusion = diffusions_table[diffusions_table['id'] == self.table['episode_id']]
            # Download podcast
            podcast_url = target_diffusion['podcastEpisode'].iloc[0]
            podcast_path = os.path.join('raw_data', os.path.basename(podcast_url))
            with requests.get(podcast_url, stream=True) as r:
                r.raise_for_status()
                with open(podcast_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Upload podcast to google storage
            blob = gsClient.upload_blob(podcast_path, bucket_name, 'data')

            # Update episode table with storage link
            blob_uri = f'gs://{bucket_name}/{blob.name}'
            self.bqClient.update_table(dataset,
                                       episode_table, 'id',
                                       self.table['episode_id'], 'lien_mp3_google_storage',
                                       blob_uri)
