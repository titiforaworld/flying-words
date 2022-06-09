import os
from flying_words.flow import build_flow

gcp_credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
gcp_project = os.getenv('GCP_PROJECT')
gcp_bucket = os.getenv('GCP_BUCKET')
azure_token = os.getenv('AZURE_TOKEN')

env_vars = dict(gcp_credentials_path=gcp_credentials_path,
            gcp_project=gcp_project,
            gcp_bucket=gcp_bucket,
            azure_token=azure_token)

flow = build_flow(env_vars)

flow.run()
