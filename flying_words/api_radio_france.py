import time
from datetime import datetime, timedelta
import requests
import pandas as pd


class ApiRadioFrance:
    """A class for retrieving information from Radio France API"""

    def __init__(self, access_token: str):
        """Constructor for ApiRadioFrance class."""

        self.access_token = access_token
        self.endpoint = f"https://openapi.radiofrance.fr/v1/graphql?x-token={self.access_token}"


    def get_yesterday_grid(self, station_name="FRANCECULTURE"):
        """Retrieve previous day grid information for a given radio station."""

        # Get previous day time window
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date_epoch = int((today - timedelta(days=1)).timestamp())
        end_date_epoch= int((today - timedelta(seconds=1)).timestamp())

        #query to retrieve grid info from API
        query_yesterday_grid = """query {
            paginatedGrid(start: TAG_START_DATE_EPOCH, end: TAG_END_DATE_EPOCH, station: TAG_STATION_NAME) {
                cursor
                node {
                    steps {
                        ... on DiffusionStep {
                            id
                            diffusion {
                                id
                                title
                                standFirst
                                url
                                published_date
                                podcastEpisode {
                                    id
                                    title
                                    url
                                    created
                                    duration
                                }
                            }
                        }
                        ... on TrackStep {
                            id
                            track {
                                id
                title
                albumTitle
              }
            }
            ... on BlankStep {
              id
              title
            }
          }
        }
      }
    }"""

        query_yesterday_grid = query_yesterday_grid.replace("TAG_START_DATE_EPOCH",str(start_date_epoch)) \
                                                   .replace("TAG_END_DATE_EPOCH",str(end_date_epoch)) \
                                                   .replace("TAG_STATION_NAME",str(station_name))

        # Query the API
        response = requests.post(self.endpoint, json={"query": query_yesterday_grid})

        diffusions_json = response.json()["data"]["paginatedGrid"]["node"]["steps"]

        # Transform response from API to Dataframe
        diffusions = []
        for diffusion in diffusions_json:
            if "diffusion" in " ".join(list(diffusion.keys())) :
                diffusions.append(pd.Series(diffusion["diffusion"], index=diffusion["diffusion"].keys()))

        diffusions_df = pd.DataFrame(diffusions)
        diffusions_df.dropna(subset=['url'], inplace=True)

        # ## creation of a dataframe without nonetype
        # emission_df_none=pd.DataFrame(columns=diffusions_df.columns)
        # for j in range(diffusions_df.shape[0])  :
        #     if diffusions_df["url"][j]!= None:
        #         emission_df_none=emission_df_none.append(diffusions_df.iloc[j], ignore_index=True)

        # Get show URL from diffusion URL
        def get_show_url(url : str):
            return url[:url.rfind('/')]

        diffusions_df["show_url"] = diffusions_df["url"].map(get_show_url)

        # Create a grid date column
        diffusions_df["grid_date"] = start_date_epoch

        return diffusions_df


    def get_episodes_to_df(self,url,bqClient):
        '''
        for a given show url - query the open API and get the last 10th episodes
        return a dictionnary containing to dataframe
        ==> df_emission: a dataframe for BQ table "episode"
        ==> df_guest: a dataframe for BQ table "episode_personnalities_V2"
        '''
        query_4 = '''query {
            showByUrl(url: "url_to_replace") {
            id
            title
            url
            standFirst
            diffusionsConnection {
                edges {
                node {
                    id
                    title
                    url
                    published_date
                    podcastEpisode {
                    url
                    title
                    }
                    personalitiesConnection {
                    edges {
                        relation
                        info
                        node {
                        id
                        name
                        }
                    }
                    }
                    taxonomiesConnection {
                    edges {
                        relation
                        info
                        node {
                        id
                        path
                        type
                        title
                        standFirst
                        }
                    }
                    }
                }
                }
            }
            taxonomiesConnection {
                edges {
                relation
                info
                node {
                    id
                    path
                    type
                    title
                    standFirst
                }
                }
            }
            }
        }'''


        query_4=query_4.replace("url_to_replace",url)
        endpoint = self.endpoint


        r4 = requests.post(endpoint, json={"query": query_4})
        episodes =r4.json()['data']["showByUrl"]["diffusionsConnection"]["edges"]
        emission=[pd.DataFrame.from_dict(episodes[i]["node"]).iloc[0] for i in range(len(episodes))]
        nom_emission=[pd.DataFrame.from_dict(episodes[i]["node"]).iloc[1]["podcastEpisode"] for i in range(len(episodes))]
        df_emission=pd.DataFrame(emission)
        df_emission["nom_emission"]=nom_emission
        df_emission["lien_mp3_google_storage"]="to be filled"

        ###filter to avoid uploading existing episode

        existing_id=bqClient.get_table("flying_words", "episode")["id"].unique()


        df_emission=df_emission[df_emission.id.isin(existing_id)==False]


        #### DF guest
        dfguest= pd.DataFrame(columns={"relation", "episode_id" ,"name","info"})
        for i in range(len(episodes)) :
            episode_id = episodes[i]["node"]["id"]
            episode_perso=  episodes[i]["node"]["personalitiesConnection"]["edges"]
            for j in range(len(episode_perso)):
                dict_new= {
                    "relation": episode_perso[j]["relation"],
                    "episode_id":episode_id,
                    "name":episode_perso[j]["node"]["name"],
                    "info":episode_perso[j]["info"]}
                guest_series = pd.Series(dict_new,name="personality")
                dfguest=dfguest.append(guest_series,ignore_index=True)

        dfguest=dfguest[dfguest.episode_id.isin(existing_id)==False]

        return  {"df_emission": df_emission, "df_guest":dfguest}
