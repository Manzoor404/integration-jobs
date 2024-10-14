# HDFS to GCS Data Migration

This project demonstrates migrating data from Hadoop HDFS to Google Cloud Storage (GCS) using PySpark. The data is read from HDFS, processed, and stored in GCS. A comparison is done to verify that the data migration is successful and that the data integrity is maintained.

## Architecture Diagram


## Requirements

- PySpark
- Google Cloud Storage (GCS)
- Hadoop HDFS
- Google Cloud Service Account JSON key

## How to Run

1. **Set up environment**:
    - Set the Google Cloud credentials:
      ```bash
      export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
      ```

2. **Run the PySpark job**:
    ```bash
    spark-submit hdfs_to_gcs_migration.py
    ```

3. **Check migration**:
    - The script compares record counts between HDFS and GCS to ensure data integrity.

## Code Summary

- The script reads a CSV file from HDFS and writes it to a GCS bucket.
- It compares the total number of records between HDFS and GCS to ensure successful migration.
