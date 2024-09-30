from pyspark.sql import SparkSession
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SpotifyETL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/hadoop-aws-3.3.1.jar,"
                          "/Users/syedmanzoor/Downloads/aws-java-sdk-bundle-1.11.901.jar,"
                          "/Users/syedmanzoor/Downloads/redshift-jdbc42-2.1.0.30/redshift-jdbc42-2.1.0.30.jar") \
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

s3_path = "s3a://syedmanzoor/staging_data/Spotify/ARR/top_100_tracks/"

# write the dataframe into S3
spotify_df_search.write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(s3_path)

print("Data loaded to S3.")

# Redshift connection details
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439
redshift_table = "spotify_arr"

# Redshift JDBC connection URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}


# load the data to Redshift
spotify_df_search.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()

print("Data loaded to Redshift")

