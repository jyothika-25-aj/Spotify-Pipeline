from kafka import KafkaProducer
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Spotify credentials
CLIENT_ID = 'your client id'
CLIENT_SECRET = 'your client secret'

# Initialize Spotify
client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Initialize Kafka producer
producer = KafkaProducer(
  bootstrap_servers='localhost:9092',
  value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch and send top tracks
def get_artist_top_tracks(artist_name):
  result = sp.search(q=artist_name, type='artist', limit=1)
  if not result['artists']['items']:
      print("Artist not found.")
      return
  
  artist = result['artists']['items'][0]
  artist_id = artist['id']
  top_tracks = sp.artist_top_tracks(artist_id)

  for track in top_tracks['tracks']:
    data = {
        'artist': artist['name'],
        'track': track['name'],
        'url': track['external_urls']['spotify']
       }
    print(data)
    producer.send('spotify_tracks', value=data)

  producer.flush()

# Example usage
get_artist_top_tracks("radiohead")
