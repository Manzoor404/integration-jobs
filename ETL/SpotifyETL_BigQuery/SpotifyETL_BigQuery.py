from pyspark.sql import SparkSession
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SpotifyETL") \
    .config("spark.jars", 
            "/Users/syedmanzoor/Downloads/gcs-connector-hadoop2-2.2.5-shaded.jar,"
            "/Users/syedmanzoor/Downloads/google-oauth-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-api-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-http-client-1.39.2.jar,"
            "/Users/syedmanzoor/Downloads/google-auth-library-oauth2-http-0.20.0.jar,"
            "/Users/syedmanzoor/Downloads/spark-bigquery-with-dependencies_2.12-0.30.0.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json") \
    .getOrCreate()

# Fetch Spotify Developer credentials from environment variables
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
scope = "user-library-read"

print("Client ID: ", client_id)
print("Client Secret: ", client_secret)
print("Redirect URI: ", redirect_uri)

# Authenticate and initialize Spotify API client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                               client_secret=client_secret,
                                               redirect_uri=redirect_uri,
                                               scope=scope))


# Function to search for artist and return the artist ID
def search_artist(artist_name):
    results = sp.search(q=artist_name, type='artist', limit=1)
    artist = results['artists']['items'][0]
    artist_id = artist['id']
    print(f"Artist Name: {artist['name']}, Artist ID: {artist_id}")
    return artist_id

# Search for AR Rahman and get artist ID dynamically
artist_name = "AR Rahman"
artist_id = search_artist(artist_name)


# Function to get top tracks for a given artist ID (10 tracks)
def get_artist_top_tracks(artist_id):
    results = sp.artist_top_tracks(artist_id)
    track_items = results['tracks']

    extracted_data = []
    for track in track_items:
        extracted_data.append({
            'track_name': track['name'],
            'album': track['album']['name'],
            'artist': ', '.join([artist['name'] for artist in track['artists']]),
            'popularity': track['popularity'],
            'duration_ms': track['duration_ms']
        })

    return extracted_data

# Fetch top tracks using the dynamically fetched artist ID for AR Rahman
top_tracks_data = get_artist_top_tracks(artist_id)

# Convert the extracted data for top tracks to a Spark DataFrame
spotify_df_top_tracks = spark.createDataFrame(top_tracks_data)
top_tracks_records = spotify_df_top_tracks.count()
print(f"Total top track records: {top_tracks_records}")

# Show the extracted data for top tracks
spotify_df_top_tracks.show()


# Function to search tracks by artist name (for fetching more than 50 tracks using pagination)
def search_tracks_by_artist_paginated(artist_name, total_limit=100):
    offset = 0
    batch_size = 50  # Spotify API limit per request is 50
    all_tracks = []

    while len(all_tracks) < total_limit:
        results = sp.search(q=f'artist:{artist_name}', type='track', limit=batch_size, offset=offset)
        track_items = results['tracks']['items']
        if not track_items:
            break  # Stop if no more tracks are returned
        all_tracks.extend(track_items)
        offset += batch_size

    extracted_data = []
    for track in all_tracks:
        extracted_data.append({
            'track_name': track['name'],
            'album': track['album']['name'],
            'artist': ', '.join([artist['name'] for artist in track['artists']]),
            'popularity': track['popularity'],
            'duration_ms': track['duration_ms']
        })

    return extracted_data

# Example: Fetch up to 100 tracks for AR Rahman using the paginated search endpoint
spotify_data_search = search_tracks_by_artist_paginated("AR Rahman", total_limit=100)

# Convert the extracted data from search results to a Spark DataFrame
spotify_df_search = spark.createDataFrame(spotify_data_search)
search_records = spotify_df_search.count()
print(f"Total search track records: {search_records}")

# Show the extracted data from search results
spotify_df_search.show(100)

# Writing the Data into GCS
spotify_df_search.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/Spotify/ARR")

print("Data stored in GCS")

# Load data into BigQuery
bigquery_table = "zomatoeda-435904.Spotify.spotify_tracks_arr"

spotify_df_search.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()

print("Data Loaded into BigQuery successfully!")
