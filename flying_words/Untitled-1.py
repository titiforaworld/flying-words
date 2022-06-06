
from flying_words.google_clients import BigQueryClient
bqClient=BigQueryClient(project_name,credential_path)
token = "d3709a1f-54b8-454f-b22f-2f1299d793fd"
url="https://www.radiofrance.fr/franceculture/podcasts/l-esprit-public"
from flying_words.api_radio_france import ApiRadioFrance
from datetime import datetime, timedelta


session =ApiRadioFrance(token)
grid = session.get_yesterday_grid()
