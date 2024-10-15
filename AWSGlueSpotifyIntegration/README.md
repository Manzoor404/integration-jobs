# Spotify to Redshift ETL using AWS Glue

In this project, we extract data from Spotify, transform it, and load it into Amazon Redshift. The ETL process is run in AWS Glue, and the job is scheduled to run daily. The ETL job also writes data to an S3 bucket as an intermediate step.

## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/7a98cefd-ffa8-451d-98f0-a77f10fadd09)


## Requirements

- **Libraries Used:**
    - `spotipy`
    - `boto3`
    - `pyspark`

Make sure the respective JARs are added in the AWS Glue path:
- `redshift-jdbc42-2.1.0.30.jar`
- Other necessary dependencies like the Spark libraries and AWS SDK.

## AWS Glue ETL Steps

1. **Spotify Data Extraction:**
    - Extract data from Spotify using the Spotify API. 
    - The authentication is handled via AWS Secrets Manager to fetch the Spotify API credentials.

2. **Data Transformation:**
    - Transform the data using PySpark, such as converting it into a Spark DataFrame.

3. **Data Storage in S3:**
    - The transformed data is saved as a CSV file in an S3 bucket for intermediate storage.

4. **Data Load to Redshift:**
    - The CSV data is then loaded into Amazon Redshift using the Redshift JDBC driver.

5. **Scheduling with AWS Glue:**
    - The ETL job is scheduled to run daily using AWS Glue.

## How to Run

1. **Run the ETL job in AWS Glue:**
    - Configure AWS Glue with the necessary libraries.
    - Set up the AWS Secrets Manager with the Spotify credentials.

2. **Daily Job Scheduling:**
    - The Glue job can be scheduled to run daily using AWS Glue triggers.

3. **Jenkins Pipeline:**
    - The Jenkins pipeline uploads the Glue ETL script to S3 for execution.
    - It fetches the script from GitHub and uploads it to the specified S3 bucket.

4. **Run the Pipeline:**
    - The Jenkins pipeline script ensures that the Glue ETL script is uploaded to S3:
    ```bash
    aws s3 cp AWSGlueSpotifyIntegration/glue_spotify_integration.py s3://syedmanzoor/staging_data/Spotify/GlueSpotify/glue_spotify_integration.py
    ```

## Code Overview

- **Spotify API Extraction:** 
    - The Spotify API is used to extract top tracks for the artist (AR Rahman in this case).
- **Transform Data:** 
    - Data is transformed and loaded into an S3 bucket as a CSV.
- **Redshift Load:** 
    - The data is then loaded into an Amazon Redshift table using the Redshift JDBC driver.
