import time
from datetime import datetime, timedelta
import requests
import pandas as pd



from flying_words.google_clients import BigQueryClient
# Variables for GCP
project_name = 'intense-elysium-346915'
bucket_name = 'le-wagon-project-75667-antoine'
credential_path = '/content/drive/MyDrive/projetWagon/env/intense-elysium-346915-f2127c89e62b.json'

# Instanciate google client
BQClient =  BigQueryClient(project_name, credential_path)




class ApiRadioFrance:
    """A class for retrieving information from Radio France API"""

    def __init__(self, access_token: str):
        """Constructor for ApiRadioFrance class."""

        self.access_token = access_token
        self.endpoint = f"https://openapi.radiofrance.fr/v1/graphql?x-token={self.access_token}"


   # def get_emission_url(url : str):
  #    return url[:url.rfind('/')]

    def get_yesterday_grid(self, station_name="FRANCECULTURE"):
        """ retrieve grid information for 24h for a day and a station name

        """
        today=datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)


        #convert day to unixtime
            ## beginning ==> midnight
        start_date_epoch = int((today - timedelta(days=1)).timestamp())
               ## beginning+ 24h
        end_date_epoch= int((today - timedelta(seconds=1)).timestamp())

        #query to retrieve grid info from API
        query_grid_emission = """query {
      paginatedGrid(start: unixtime_beginning, end: unixtime_end_24h, station: station_name) {
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
    }
    """
        #replace parameters in the query
        query_grid_emission=query_grid_emission.replace("unixtime_beginning",str(start_date_epoch))
        query_grid_emission= query_grid_emission.replace("unixtime_end_24h",str(end_date_epoch))
        query_grid_emission= query_grid_emission.replace("station_name",str(station_name))

        #query to the API
        r4 = requests.post(self.endpoint, json={"query": query_grid_emission})


        episode= r4.json()["data"]["paginatedGrid"]["node"]["steps"]

        #start to transform response from API to Dataframe

        emission =[]
        for i in range(len(episode)) :
            if  "diffusion" in " ".join(list(episode[i].keys())) :
                emission.append(pd.Series(episode[i]["diffusion"], index=episode[i]["diffusion"].keys()))

        emission_df=pd.DataFrame(emission)

        ## creation of a dataframe without nonetype
        emission_df_none=pd.DataFrame(columns=emission_df.columns )


        for j in range(emission_df.shape[0])  :
            if emission_df["url"][j]!= None:
                emission_df_none=emission_df_none.append( emission_df.iloc[j], ignore_index=True)

        #get emission url thanks to episode url
        def get_emission_url(url : str):
          return url[:url.rfind('/')]
        emission_df_none["url_emission"]=emission_df_none["url"].map(get_emission_url)


        #create a grid date column
        emission_df_none["grid_date"]=start_date_epoch

        return emission_df_none

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
