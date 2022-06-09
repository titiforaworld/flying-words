import os
from click import Tuple
from colorama import Fore, Style

from prefect import task, Flow, context
from prefect.core.parameter import Parameter

from flying_words.google_clients import StorageClient, BigQueryClient
from flying_words.audio import merge_diffusion_with_samples
from flying_words.target import Target
from flying_words.diarization import Diarization
from flying_words.speaker import Speaker
from flying_words.transcription import Transcription


logger = context.get("logger")


@task(nout=2)
def connect_google_clients(project_name: str, credential_path: str) -> Tuple:

    print(Fore.GREEN + "\n# 🐙 Prefect task - Connect Google Storage and BigQuery clients:" + Style.RESET_ALL)

    gsClient = StorageClient(project = project_name, credentials = credential_path)
    bqClient = BigQueryClient(project = project_name, credentials = credential_path)

    return (gsClient, bqClient)


@task
def get_target(bqClient: BigQueryClient, gsClient: StorageClient, bucket_name):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Get target:" + Style.RESET_ALL)

    # Get target and update it
    target = Target(bqClient)

    target.update_target_diffusion_storage_link(gsClient, bucket_name)
    logger.info('Target diffusion storage link updated')

    target.load_table()

    return target

@task
def diffusion_samples_merger(target: Target, gsClient: StorageClient):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Get info for segmentation:" + Style.RESET_ALL)

    merged_diffusion_info = merge_diffusion_with_samples(target.table, gsClient)
    logger.info('Voice samples and diffusion merged')

    return merged_diffusion_info


@task
def diffusion_diarization(merged_audio_info):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Segment target diffusion:" + Style.RESET_ALL)

    # Diarization
    diffusion_diarization = Diarization(merged_audio_info['merged_audio'])
    diffusion_diarization.make_diarization(min_duration_off = 1.0)
    diffusion_diarization_df = diffusion_diarization.get_diarization_df()
    logger.info('Audio diarized')

    return diffusion_diarization_df


@task
def speaker_sampler(diffusion_diarization_df,
                    merged_diffusion_info,
                    bqClient: BigQueryClient,
                    gsClient: StorageClient,
                    bucket_name):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Make speaker sampling:" + Style.RESET_ALL)

    speaker = Speaker()

    speaker.get_unknown_info(diffusion_diarization_df, merged_diffusion_info)
    logger.info('Unknown information get')

    # get retreated dataframe
    retreated_df = speaker.get_retreated_dataframe(diffusion_diarization_df, merged_diffusion_info)
    logger.info('Transformed diazrization dataframe to take merged samples into account')

    # upload retreated dataframe to Big Query
    bqClient.append_row_to_table(dataset='flying_words',  input_df = retreated_df, dest_table='segmentation')
    logger.info('Uploaded diarization to Big Query')

    # upload speaker samples to Could Storage
    sample_dataset = "personnality_sample"
    speaker.upload_samples_tables(audio_file=merged_diffusion_info['diffusion_audio'],
                                  gsClient=gsClient,
                                  big_query=bqClient,
                                  bucket_name=bucket_name,
                                  sample_dataset=sample_dataset)

    logger.info('Uploaded speaker samples to GCP')


@task(nout=2)
def transcription(target: Target,
                  merged_audio_info,
                  bqClient: BigQueryClient,
                  gsClient: StorageClient,
                  bucket_name):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Transcript diffusion:" + Style.RESET_ALL)

    transcription = Transcription(merged_audio_info['diffusion_audio'], target.table['episode_id'])
    transcription.make_transcription()

    transcript_blob_uri, transcript_dict_blob_uri = transcription.upload_to_gcp(gsClient, bucket_name, bqClient)

    return (transcript_blob_uri, transcript_dict_blob_uri)

@task
def get_result(target: Target,
               bqClient: BigQueryClient,
               gsClient: StorageClient, bucket_name,
               transcript_blob_uri,
               transcript_dict_blob_uri):

    print(Fore.GREEN + "\n# 🐙 Prefect task - Get result:" + Style.RESET_ALL)

    transcript_dict_path = os.path.join('raw_data', 'transcript.txt')
    transcript_df = gsClient.get_transcript_df(transcript_dict_blob_uri, transcript_blob_uri, transcript_dict_path)

    result = bqClient.words_diarization_info_merger(transcript_df, target.table['episode_id'], bqClient)

    # Create results folder if doesn't exist
    results_folder_path = os.path.join('raw_data', 'results')
    os.makedirs(results_folder_path, exist_ok=True)

    # Create result CSV
    result_filename = f"result_{target.table['episode_id']}.csv"
    result_path = os.path.join(results_folder_path, result_filename)
    result.to_csv(result_path)

    # Upload result CSV
    blob = gsClient.upload_blob(result_path, bucket_name, 'result')
    blob_uri = f'gs://{bucket_name}/{blob.name}'

    bqClient.update_table('flying_words', 'episode', 'id', target.table['episode_id'],
                          'enhanced_transcription', blob_uri)


def build_flow(env_vars):
    """
    build the prefect workflow for 'flying_words' package
    """

    with Flow('flying_words_flow') as flow:

        gsClient, bqClient = connect_google_clients(env_vars['gcp_project'],
                                                    env_vars['gcp_credentials_path'])

        target = get_target(bqClient, gsClient, env_vars['gcp_bucket'])

        merged_audio_info = diffusion_samples_merger(target, gsClient)

        segmented_diffusion_df = diffusion_diarization(merged_audio_info)

        speaker_sampler(segmented_diffusion_df,
                        merged_audio_info,
                        bqClient,
                        gsClient,
                        env_vars['gcp_bucket'])

        transcript_blob_uri, transcript_dict_blob_uri = transcription(target,
                                                                      merged_audio_info,
                                                                      bqClient,
                                                                      gsClient,
                                                                      env_vars['gcp_bucket'])

        get_result(target,
                   bqClient,
                   gsClient,
                   env_vars['gcp_bucket'],
                   transcript_blob_uri,
                   transcript_dict_blob_uri,)

    return flow
