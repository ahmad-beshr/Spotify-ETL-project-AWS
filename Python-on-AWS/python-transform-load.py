import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

# read albums data function 
def albums(data):
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
    return album_list

# read artists data
def artists(data):
    artist_list = []
    for row in data['items']:
        for key, val in row.items():
            if key=='track':
                for artist in val['artists']:
                    artist_dict = {'artist_name' : artist['name'], 
                                   'artist_id' : artist['id'],
                                   'external_url': artist['href']}
    
                    artist_list.append(artist_dict)
    return artist_list
    
# read songs data
def songs(data):
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
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotifypro1"
    Key = "raw_data/to_be_processed/"
    
    # read json file contents from s3 buckets
    spotify_data = []
    spotify_keys = []
    for i in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = i['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_data = albums(data)
        artist_data = artists(data)
        song_data = songs(data)        
        
        # convert dictionaries into dataframes
        album_df = pd.DataFrame(album_data)
        artist_df = pd.DataFrame(artist_data)
        song_df = pd.DataFrame(song_data)
        
        # drop duplicates in IDs of each dataframe
        album_df = album_df.drop_duplicates(subset = ['album_id'])
        artist_df = artist_df.drop_duplicates(subset = ['artist_id'])
        
        # convert release_date from object Dtype to datatime
        #album_df.loc[:,'release_date'] = pd.to_datetime(album_df.loc[:, 'release_date'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        
        # convert song_added date Dtype to datetime
        #song_df.loc[:, 'song_added'] = pd.to_datetime(song_df.loc[:, 'song_added'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')
        
        # datetime now
        date_time_now = datetime.now().strftime("%m-%d-%Y %H:%M:%S") # current date and time        
        
        # Load the song data into S3 
        song_key = "transformed_data/songs_data/song_transformed_" + date_time_now + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = song_key, Body = song_content)
        
        # Load the Album data into s3
        album_key = "transformed_data/album_data/album_transformed_" + date_time_now + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
        
        # Load the Artist data into s3
        artist_key = "transformed_data/artist_data/transformed_" + date_time_now + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_content)
    
    # Now load the data into 'processed' folder
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key' : key
        }
       
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
        
