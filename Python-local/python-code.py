# uncomment below line for the first time to install spotipy if it's not installed
#!pip install spotipy

import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials

# credentials
client_cred = SpotifyClientCredentials(client_id = "<your spotify client id>", client_secret = "<your spotify client secret id>")

# spotify object
sp = spotipy.Spotify(client_credentials_manager = client_cred)

# playlist URL. feel free to look up your favorite playlist
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbKj23U1GF4IR"

# get list id
playlist_id = playlist_link.split("/")[-1]
playlist_id

# get the tracks of the playlist
data = sp.playlist_tracks(playlist_id)

# get albums details
album_list = []
for row in data['items']:
    album_id = row['track']['album']['id']
    name = row['track']['album']['name']
    release_date = row['track']['album']['release_date']
    total_tracks = row['track']['album']['total_tracks']
    url = row['track']['album']['external_urls']['spotify']
    album_elements = {'album_id': album_id, 
                     'name': name,
                     'release_date': release_date,
                     'total_tracks': total_tracks,
                     'url': url}
    album_list.append(album_elements)

# get artists details
artist_list = []
for row in data['items']:
    for key, val in row.items():
        if key=='track':
            for artist in val['artists']:
                artist_dict = {'artist_name' : artist['name'], 
                               'artist_id' : artist['id'],
                               'external_url': artist['href']}

                artist_list.append(artist_dict)

# get songs details
song_list = []
for row in data['items']:
    #print(row)
    song_id = row['track']['id']
    song_name = row['track']['name']
    song_duration = row['track']['duration_ms']
    song_url = row['track']['external_urls']['spotify']
    song_popularity = row['track']['popularity']
    song_added = row['added_at']
    album_id = row['track']['album']['id']
    artist_id = row['track']['album']['artists'][0]['id']
    song_element = {'song_id': song_id,
                   'song_name' : song_name,
                   'song_duration': song_duration,
                   'song_popularity': song_popularity,
                   'song_added': song_added,
                   'album_id': album_id,
                   'artist_id': artist_id,
                   }    
    song_list.append(song_element)


# Convert Dictionaries into DataFrames

album_df = pd.DataFrame(album_list)
album_df.head()

artist_df = pd.DataFrame(artist_list)
artist_df.head()

song_df = pd.DataFrame(song_list)
song_df.head()

# drop duplicates in IDs of album and artist dataframes
album_df = album_df.drop_duplicates(subset = ['album_id'])
artist_df = artist_df.drop_duplicates(subset = ['artist_id'])

# print the info of DFs
album_df.info()

# convert release_date from object Dtype to datatime
# album_df['release_date'] = pd.to_datetime(album_df['release_date'])

# Note: above conversion might give a warning. a better way as per Pandas warning is to perform the Dtype conversion as below:
album_df.loc[:,'release_date'] = pd.to_datetime(album_df.loc[:, 'release_date'])
album_df.head()

# change song_added Dtype to datetime
song_df.loc[:, 'song_added'] = pd.to_datetime(song_df.loc[:, 'song_added'])
song_df.info()

# export to CSV
song_df.to_csv('../song.csv', index = False)
album_df.to_csv('../album.csv', index = False)
artist_df.to_csv('../artist.csv', index = False)
