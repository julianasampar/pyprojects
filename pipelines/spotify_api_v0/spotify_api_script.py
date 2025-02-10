## This script define the functions to be called on Spotify API DAG

import spotipy
import numpy as np 
from spotipy.oauth2 import SpotifyClientCredentials
import pipelines.utils.personal_env as penv
import pandas as pd
from datetime import datetime
import pipelines.utils.common as common

# Stablishing Spotify Authentication
auth_manager = SpotifyClientCredentials(client_id=penv.spotify_client_id, client_secret=penv.spotify_client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)


def get_new_releases(ti, **kwargs):
    # Defining function to get new releases - it returns all albums released 2 weeks from now
    
    # Creating a list of all available markets on Spotify

    markets = [
            "AD", "AE", "AG", "AL", #"AM", "AO", "AR", "AT", "AU", "AZ", "BA", "BB", "BD", 
            #"BE", "BF", "BG", "BH", "BI", "BJ", "BN", "BO", "BR", "BS", "BT", "BW", "BY", 
            #"BZ", "CA", "CD", "CG", "CH", "CI", "CL", "CM", "CO", "CR", "CV", "CW", "CY", 
            #"CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "ES", "ET", "FI", 
            #"FJ", "FM", "FR", "GA", "GB", "GD", "GE", "GH", "GM", "GN", "GQ", "GR", "GT", 
            #"GW", "GY", "HK", "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IN", "IQ", "IS", 
            #"IT", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KR", "KW", "KZ", 
            #"LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", 
            #"MD", "ME", "MG", "MH", "MK", "ML", "MN", "MO", "MR", "MT", "MU", "MV", "MW", 
            #"MX", "MY", "MZ", "NA", "NE", "NG", "NI", "NL", "NO", "NP", "NR", "NZ", "OM", 
            #"PA", "PE", "PG", "PH", "PK", "PL", "PR", "PS", "PT", "PW", "PY", "QA", "RO", 
            #"RS", "RW", "SA", "SB", "SC", "SE", "SG", "SI", "SK", "SL", "SM", "SN", "SR", 
            #"ST", "SV", "SZ", "TD", "TG", "TH", "TJ", "TL", "TN", "TO", "TR", "TT", "TV", 
            #"TW", "TZ", "UA", "UG", "US", "UY", "UZ", "VC", "VE", "VN", "VU", "WS", "XK", 
            #"ZA", "ZM", "ZW"
           ]
    
    ## Creating empty DataFrame to append API values after request

    releases = pd.DataFrame()
    
    ## Creating loop to make GET Request
    ## The first request gets the list of new Albums released two weeks ago from each market defined above

    for i in range(len(markets)):
        
        ## The Spotify only returns 50 values per request 
        # (the variables 'limit' and 'batchSize' helps Spotify not crash if the data exceeds)
        
        limit = 50
        offset = 0
        
        while offset < 1000:    # Spotify limit for Search Request is 1000
            
            ## Making GET request of the type search with the tag:'new', that returns the latest Albums
            newReleases = sp.search(q="tag:new", market=markets[i], type="album", limit=limit, offset=offset)
            newReleasesData = pd.DataFrame.from_dict(newReleases['albums']['items'])
            
            releases = pd.concat([releases, newReleasesData])
            releases['extractionTimestamp'] = datetime.today().strftime('%Y-%m-%d %X')
            
            # Incremental addition to offset to return the following pages of data
            offset=offset+limit
            
        print("Successfully got request from ", markets[i], "market")
        
    ti.xcom_push(key='spotify_new_releases_df', value=releases)
    


def prep_new_releases(ti, **kwargs):
    # Defining function to treat releases dataframe
    # The preparation is being made on another task to avoid unnecessary requests.
    # If the preparation fails, we don't need to make the request again
    
    releases = ti.xcom_pull(key='spotify_new_releases_df', task_ids='get_new_releases')
    releases = pd.DataFrame(releases)

    releases['spotify_url'] = releases['external_urls'].apply(lambda x: x['spotify'] if isinstance(x, dict) else None)

    # Here, we are maintaining the id, uri and href in lists to facilitate in case we need to use them on other requests
    releases['artist_uri'] = releases['artists'].apply(lambda artists: [artist['uri'] for artist in artists])
    releases['artist_href'] = releases['artists'].apply(lambda artists: [artist['href'] for artist in artists])
    releases['artist_id'] = releases['artists'].apply(lambda artists: [artist['id'] for artist in artists])
    
    # Dropping unnecessary and/or treated columns
    releases = releases.drop(columns=['external_urls', 'artists'])

    # Ordering columns
    releases = releases[[
                        'id', 
                        'href', 
                        'uri', 
                        'spotify_url', 
                        'album_type', 
                        'total_tracks', 
                        'is_playable', 
                        'name', 
                        'release_date', 
                        'release_date_precision', 
                        'type', 
                        'artist_id', 
                        'artist_href', 
                        'artist_uri', 
                        'images', 
                        'extractionTimestamp']
                ]
    
    # Removing duplicated release id 
    releases = releases[releases.duplicated(subset='id') == False]
    artistsList = list(releases['artist_id'].explode())
    albumsList = list(releases['id'])
    
    ## Creating Xcoms to send the data forward 
    ti.xcom_push(key='spotify_new_releases_df', value=releases)
    ti.xcom_push(key='spotify_artists_id_list', value=artistsList)
    ti.xcom_push(key='spotify_albums_id_list', value=albumsList)
    
    


def get_artists(ti, **kwargs):
    # Defining function to get data from new realeses artists'
    
    ## Getting the list of artists Id from the previous function to make the API call
    artistsList = ti.xcom_pull(key='spotify_artists_id_list', task_ids='prep_new_releases')
    artistsList =  list(set(artistsList))
    
    ## Creating empty DataFrame to append API values after request
    artists = pd.DataFrame()
    
    batchSize = 50 # Spotify limit for Artist Request is 50
        
    for j in range(0, len(artistsList), batchSize):
        artistsBatch = artistsList[j:j + batchSize]
        artistsData = sp.artists(artistsBatch)
        artistsData = pd.DataFrame.from_dict(artistsData['artists'])  
        artists = pd.concat([artists, artistsData])
        artists['extractionTimestamp'] = datetime.today().strftime('%Y-%m-%d %X')
        
        print("Successfully got ", round(((j + batchSize)/len(artistsList)) * 100, 2), "% of Artists")
        
        ti.xcom_push(key='spotify_artists_df', value=artists)



def prep_artists(ti, **kwargs):
    
    artists = ti.xcom_pull(key='spotify_artists_df', task_ids='get_artists')
    artists = pd.DataFrame(artists)
    
    ## Treating some fields (renaming, exploding the dicts, etc)
    artists['spotify_url'] = artists['external_urls'].apply(lambda x: x['spotify'] if isinstance(x, dict) else None)
    artists['followers'] = artists['followers'].apply(lambda x: x['total'] if isinstance(x, dict) else None)
    
    # Dropping unnecessary and/or treated columns
    artists = artists.drop(columns=['external_urls'])

    # Ordering columns
    artists = artists[[
                        'id', 
                        'href', 
                        'uri', 
                        'spotify_url', 
                        'type',
                        'name', 
                        'followers', 
                        'popularity', 
                        'genres', 
                        'images', 
                        'extractionTimestamp']
    ]
    
    # Removing duplicated artist id 
    artists = artists[artists.duplicated(subset='id') == False]
    
    ti.xcom_push(key='spotify_artists_df', value=artists)
    
    

def get_albums(ti, **kwargs):
    # Defining function to get data from new realeses artists'
    
    ## Getting the list of albums Id from the previous function to make the API call    
    albumsList = ti.xcom_pull(key='spotify_albums_id_list', task_ids='prep_new_releases')
    
    ## Creating empty DataFrame to append API values after request
    albums = pd.DataFrame()
    
    # Redefining batchSize variable
    batchSize = 20 # Spotify limit for Album Request is 20

    for k in range(0, len(albumsList), batchSize):
        
        albumsData = sp.albums(albumsList[k:k + batchSize])
        albumsData = pd.DataFrame.from_dict(albumsData['albums'])
        albums = pd.concat([albums, albumsData])
        albums['extractionTimestamp'] = datetime.today().strftime('%Y-%m-%d %X')
        
        print("Successfully got ", round((k + batchSize) / len(albumsList) * 100, 2), "% of albums")
        
    ti.xcom_push(key='spotify_albums_df', value=albums)
    
    
    
def prep_albums(ti, **kwargs):
    
    albums = ti.xcom_pull(key='spotify_albums_df', task_ids='get_albums')
    albums = pd.DataFrame(albums)
    
    ## Treating some fields (renaming, extracting values, etc)
    albums['spotify_url'] = albums['external_urls'].apply(lambda x: x['spotify'] if isinstance(x, dict) else None)

    albums['artist_id'] = albums['artists'].apply(lambda artists: [artist['id'] for artist in artists])
    albums['artist_href'] = albums['artists'].apply(lambda artists: [artist['href'] for artist in artists])
    albums['artist_uri'] = albums['artists'].apply(lambda artists: [artist['uri'] for artist in artists])

    ## To extract the track id we'll have to:
    # 1) Extract the key 'items' inside the dictionary
        
    albums['items'] = albums['tracks'].apply(lambda x: x['items'] if isinstance(x, dict) else [])

    # 2) Extract the id inside the list
    albums['track_id'] = albums['items'].apply(
        lambda x: [track['id'] for track in np.array(x)] if isinstance(x, (list, np.ndarray)) else None
    )
    albums['track_href'] = albums['items'].apply(
        lambda x: [track['href'] for track in np.array(x)] if isinstance(x, (list, np.ndarray)) else None
    )
    albums['track_uri'] = albums['items'].apply(
        lambda x: [track['uri'] for track in np.array(x)] if isinstance(x, (list, np.ndarray)) else None
    )
    
    # Removing unnecessary and/or treated columns
    albums = albums.drop(columns=['external_urls', 'artists', 'tracks', 'genres']) 

    # Ordering columns
    albums = albums[[
                    'id', 
                    'href',
                    'uri',
                    'spotify_url',
                    'album_type',
                    'total_tracks',  
                    'name', 
                    'available_markets',
                    'release_date', 
                    'release_date_precision', 
                    'type', 
                    'items',
                    'artist_id', 
                    'artist_href', 
                    'artist_uri', 
                    'track_id',
                    'track_href',
                    'track_uri',
                    'popularity',
                    'label',
                    'copyrights',
                    'external_ids',                
                    'images', 
                    'extractionTimestamp']
                ]
    
    # Removing duplicated album id
    albums = albums[albums.duplicated(subset='id') == False]
    tracksList = list(albums['track_id'].explode())
    
    ti.xcom_push(key='spotify_albums_df', value=albums)
    ti.xcom_push(key='spotify_tracks_id_list', value=tracksList)

    
    
def get_tracks(ti, **kwargs):
    
    tracksList = ti.xcom_pull(key='spotify_tracks_id_list', task_ids='prep_albums')
    tracksList =  list(set(tracksList))
    
    tracks = pd.DataFrame()
    
    # Redefining batchSize variable
    batchSize = 50 # Spotify limit for Track Request is 50

    for l in range(0, len(tracksList), batchSize):
        tracksData = sp.tracks(tracksList[l:l + batchSize])
        tracksData = pd.DataFrame.from_dict(tracksData['tracks'])
        
        tracks = pd.concat([tracks, tracksData])
        
        print("Successfully got ", round((l + batchSize) / len(tracksList) * 100, 2), "% of tracks")
        
    ti.xcom_push(key='spotify_tracks_df', value=tracks)
    
    
    
def prep_tracks(ti, **kwargs):
    
    tracks = ti.xcom_pull(key='spotify_tracks_df', task_ids='get_tracks')
    tracks = pd.DataFrame(tracks)
    
    # Doing the same treatments as above    
    tracks['spotify_url'] = tracks['external_urls'].apply(lambda x: x['spotify'] if isinstance(x, dict) else None)


    tracks['artists_id'] = tracks['artists'].apply(lambda artists: [artist['id'] for artist in artists])
    tracks['artists_href'] = tracks['artists'].apply(lambda artists: [artist['href'] for artist in artists])
    tracks['artists_uri'] = tracks['artists'].apply(lambda artists: [artist['uri'] for artist in artists])

    tracks['album_id'] = tracks['album'].apply(lambda x: x['id'] if isinstance(x, dict) else [])
    tracks['album_href'] = tracks['album'].apply(lambda x: x['href'] if isinstance(x, dict) else [])
    tracks['album_uri'] = tracks['album'].apply(lambda x: x['uri'] if isinstance(x, dict) else [])
    
    tracks = tracks.drop(columns = ['album', 'artists', 'external_urls'])

    tracks = tracks[[
                    'id',
                    'href',
                    'uri',
                    'spotify_url',
                    'artists_id',
                    'artists_href',
                    'artists_uri',
                    'album_id',
                    'album_href',
                    'album_uri',
                    'available_markets',
                    'disc_number',
                    'duration_ms',
                    'explicit',
                    'external_ids',]           
        ]
    
    ti.xcom_push(key='spotify_tracks_df', value=tracks)
    
def unload_spotify_data(ti, **kwargs):
    
    releases = ti.xcom_pull(key='spotify_new_releases_df', task_ids='prep_new_releases')
    artists = ti.xcom_pull(key='spotify_artists_df', task_ids='prep_artists')
    albums = ti.xcom_pull(key='spotify_albums_df', task_ids='prep_albums')
    tracks = ti.xcom_pull(key='spotify_tracks_df', task_ids='prep_tracks')
    
    ## Defining variables to call unload_data function
    
    origin = 'spotify_api'
    bucket = penv.bucket_path
    bucket_folder = f"{penv.bucket_folder}/sp"
    file_formats = ['parquet']
    df_dict = {
            'releases': releases,
            'artists': artists,
            'albums': albums,
            'tracks': tracks    
    }
    
    ## Writing Dataframe to Bucket folder with desired file format 

    common.unload_data(origin, file_formats, df_dict, bucket, bucket_folder)
    




