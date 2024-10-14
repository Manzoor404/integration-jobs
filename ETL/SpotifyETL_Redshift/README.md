# Spotify ETL to S3 and Redshift

This project extracts top tracks and search results for AR Rahman from the Spotify API using Spotipy, processes the data using PySpark, and then stores the results in Amazon S3 and Amazon Redshift.

## Requirements

- PySpark
- Spotipy (Spotify API Client)
- AWS S3
- Amazon Redshift
- AWS SDK

## How to Run

1. **Set up environment variables for Spotify API credentials**:
    - `SPOTIFY_CLIENT_ID`
    - `SPOTIFY_CLIENT_SECRET`
    - `SPOTIFY_REDIRECT_URI`
    - Set these credentials in your environment or in your code.

2. **Run the PySpark job**:
    ```bash
    spark-submit spotify_etl_redshift_s3.py
    ```

3. **Check the Output**:
    - The top 100 tracks of AR Rahman are extracted and written to an S3 bucket in CSV format.
    - The data is also loaded into Amazon Redshift.

## Code Summary

- The script authenticates with the Spotify API to extract the top tracks for AR Rahman.
- It paginates through up to 100 tracks and converts them into a PySpark DataFrame.
- The data is written to an S3 bucket and then loaded into an Amazon Redshift table.

## Output

- Data is stored in S3 under the specified path:
    ```
    s3://syedmanzoor/staging_data/Spotify/ARR/top_100_tracks/
    ```

- Data is loaded into Redshift under the table `spotify_arr`.