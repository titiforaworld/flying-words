from flying_words.google_clients import BigQueryClient, StorageClient
import requests
import os


class Target:
    def __init__(self, bqClient: BigQueryClient):

        self.bqClient = bqClient
        self.targets_table = self.bqClient.get_table('flying_words', 'view_target_output')

    def update_target_diffusion_storage_link(self, gsClient: StorageClient, bucket_name):

        if self.targets_table.shape[0]:
            info = self.targets_table.iloc[0]
        else:
            return None

        dataset = 'flying_words'
        episode_table = 'episode'
        if info['episode_lien_mp3_google_storage'] == 'to be filled':
            diffusions_table = self.bqClient.get_table(dataset, episode_table)
            target_diffusion = diffusions_table[diffusions_table['id'] == info['episode_id']]
            # Download podcast
            podcast_url = target_diffusion['podcastEpisode'].iloc[0]
            with requests.get(podcast_url, stream=True) as r:
                r.raise_for_status()
                with open(os.path.basename(podcast_url), 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Upload podcast to google storage
            blob = gsClient.upload_blob(os.path.basename(podcast_url), bucket_name, 'data')

            # Update episode table with storage link
            blob_uri = f'gs://{gsClient.project}/{blob.name}'
            print(blob_uri)
            print(dataset, episode_table)
            print(info['episode_id'])
            self.bqClient.update_table(dataset, episode_table, 'id', info['episode_id'], 'lien_mp3_google_storage', blob_uri)
