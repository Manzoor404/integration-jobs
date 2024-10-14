# Spotify Data ETL to GCS and BigQuery

This project demonstrates how to use the Spotify API to fetch artist information and track data, and then store the results in Google Cloud Storage (GCS) and Google BigQuery using PySpark.


## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/fa0cede6-5fe3-4450-a88c-0c78bb023749)



## Requirements

- PySpark
- Spotipy (Spotify API)
- Google Cloud Storage (GCS)
- Google BigQuery
- Spotify Developer Account and API credentials

## How to Run

1. **Set up Spotify API credentials**:
    - Set the following environment variables with your Spotify Developer credentials:
      ```bash
      export SPOTIFY_CLIENT_ID="your_client_id"
      export SPOTIFY_CLIENT_SECRET="your_client_secret"
      export SPOTIFY_REDIRECT_URI="your_redirect_uri"
      ```

2. **Set up Google Cloud credentials**:
    - Ensure that the GCS and BigQuery credentials are set up and export the Google Cloud JSON key:
      ```bash
      export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
      ```

3. **Run the PySpark Job**:
    ```bash
    spark-submit spotify_to_gcs_bigquery.py
    ```

4. **ETL Workflow**:
    - The script fetches top tracks for AR Rahman from Spotify using the Spotify API.
    - The data is written to Google Cloud Storage (GCS) in CSV format.
    - The data is loaded into BigQuery for further analysis.

## Code Summary

- **Spotify API**: Fetches top tracks and search results for AR Rahman.
- **PySpark**: Transforms the data and loads it into GCS and BigQuery.
- **GCS**: Stores the data in CSV format.
- **BigQuery**: Stores the track data for analysis.

## Output

- The script prints the total records fetched and verifies successful storage in GCS and BigQuery.
