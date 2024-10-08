import os
from pyspark.sql import SparkSession
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import boto3
import json

# Initialize Spark session for AWS Glue
spark = SparkSession.builder \
    .appName("SpotifyETL") \
    .getOrCreate()

# Fetch secrets from AWS Secrets Manager
def get_secret():
    secret_name = "SpotifyAPICredentials"
    region_name = "eu-north-1"  # Adjust region name accordingly

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

# Fetch secrets for Spotify API
secret = get_secret()
client_id = secret["SPOTIFY_CLIENT_ID"]
client_secret = secret["SPOTIFY_CLIENT_SECRET"]
redirect_uri = "https://5nqaymxaeg.execute-api.eu-north-1.amazonaws.com/dev"  # Use your correct Redirect URI
scope = "user-library-read"

# Authenticate and initialize Spotify API client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                               client_secret=client_secret,
                                               redirect_uri=redirect_uri,
                                               scope=scope))

# Search for AR Rahman and get artist ID dynamically
def search_artist(artist_name):
    results = sp.search(q=artist_name, type='artist', limit=1)
    artist = results['artists']['items'][0]
    artist_id = artist['id']
    print(f"Artist Name: {artist['name']}, Artist ID: {artist_id}")
    return artist_id

artist_name = "AR Rahman"
artist_id = search_artist(artist_name)

# Fetch top tracks for the artist
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

top_tracks_data = get_artist_top_tracks(artist_id)

# Convert the extracted data to a Spark DataFrame
spotify_df_top_tracks = spark.createDataFrame(top_tracks_data)

# Show the schema and a preview of the extracted data
spotify_df_top_tracks.printSchema()
spotify_df_top_tracks.show(5)

# Write the data to S3
s3_path = "s3://syedmanzoor/staging_data/Spotify/GlueSpotify/"
spotify_df_top_tracks.write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(s3_path)

print("Data successfully written to S3.")

# Redshift connection details
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439
redshift_table = "glue_spotify_arr"

# Redshift JDBC connection URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

# Write the data to Redshift
spotify_df_top_tracks.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .mode("append") \
    .options(**connection_properties) \
    .save()

print("Data successfully loaded into Redshift!")