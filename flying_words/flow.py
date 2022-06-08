import os
from click import Tuple
from colorama import Fore, Style

from prefect import task, Flow, context
from prefect.core.parameter import Parameter

from flying_words.google_clients import StorageClient, BigQueryClient
from flying_words.audio import merge_diffusion_with_samples
from flying_words.target import Target
from flying_words.diarization import Diarization


@task(nout=2)
def connect_google_clients(project_name: str, credential_path: str) -> Tuple:

    print(Fore.GREEN + "\n# 🐙 Prefect task - Connect Google Storage and BigQuery clients:" + Style.RESET_ALL)

    gsClient = StorageClient(project = project_name, credentials = credential_path)
    bqClient = BigQueryClient(project = project_name, credentials = credential_path)

    return (gsClient, bqClient)

@task
def diffusion_segmentation(bqClient: BigQueryClient, gsClient: StorageClient, bucket_name):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Segment target diffusion:" + Style.RESET_ALL)

    logger = context.get("logger")

    # Get target and update it
    target = Target(bqClient)

    target.update_target_diffusion_storage_link(gsClient, bucket_name)
    logger.info('Target diffusion storage link updated')

    target_table = bqClient.get_table(dataset='flying_words', table_name='view_target_output')

    # Merge diffusion with voice samples
    merge_audio_info = merge_diffusion_with_samples(target_table, gsClient)
    logger.info('Voice samples and diffusion merged')

    # Diarization
    audio_wav = merge_audio_info['merged_audio']
    diarization_audiowav = Diarization(audio_wav)
    diarization_audiowav.make_diarization(min_duration_off = 1.0)
    segmentation_df = diarization_audiowav.get_diarization_df()

    return segmentation_df


def build_flow():
    """
    build the prefect workflow for 'flying_words' package
    """

    gcp_credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
    gcp_project = os.getenv('GCP_PROJECT')
    gcp_bucket= os.getenv('GCP_BUCKET')

    with Flow('flying_words_flow') as flow:
        gsClient, bqClient = connect_google_clients(gcp_project, gcp_credentials_path)
        target_table = diffusion_segmentation(bqClient, gsClient, gcp_bucket)

    return flow
