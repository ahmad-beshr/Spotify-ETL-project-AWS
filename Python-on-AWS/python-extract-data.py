import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    # credentials
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    client_cred = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
    
    # spotify object
    sp = spotipy.Spotify(client_credentials_manager = client_cred)
    
    # play list URL
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbKj23U1GF4IR"

    # get list id
    playlist_id = playlist_link.split("/")[-1]
  
    # get the tracks of the playlist
    data = sp.playlist_tracks(playlist_id)
        
    # save the data into S3 using boto3
    client = boto3.client("s3")
    
    time_now = datetime.now().isoformat(timespec='milliseconds')
    file_name = "raw_data-"+time_now+".json"
    
    client.put_object(
        Bucket="spotifypro1",
        Key = "raw_data/to_be_processed/"+file_name,
        Body = json.dumps(data)
        )
