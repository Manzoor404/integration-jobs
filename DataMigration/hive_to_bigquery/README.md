# Hive to BigQuery Data Migration

This project demonstrates migrating data from a Hive table to Google BigQuery using PySpark. The data is read from Hive, temporarily stored in Google Cloud Storage (GCS), and finally written into a BigQuery table. The script also verifies the data migration by comparing record counts between Hive and BigQuery.

## Requirements

- PySpark
- Hive
- Google Cloud Storage (GCS)
- Google BigQuery
- Google Cloud Service Account JSON Key

## How to Run

1. **Set up environment**:
    - Ensure that Google Cloud credentials are configured:
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
    ```

2. **Run the PySpark job**:
    ```bash
    spark-submit hive_to_bigquery_migration.py
    ```

3. **Check migration**:
    - The script compares record counts between Hive and BigQuery to ensure data integrity.

## Code Summary

- The script reads data from a Hive table.
- It stores the data temporarily in GCS in CSV format.
- The data is then loaded into BigQuery.
- Finally, it compares the number of records in both Hive and BigQuery to ensure that the migration was successful.

## Output

- If the migration is successful:
    ```
    Data Migration Successful: Hive and BigQuery have the same record count.
    ```

- If there is a discrepancy:
    ```
    Data Migration Warning: Record mismatch detected!
    ```