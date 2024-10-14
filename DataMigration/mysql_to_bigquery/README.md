# MySQL to BigQuery Data Migration

This project demonstrates how to migrate data from a MySQL database to Google BigQuery using PySpark. The data is first extracted from MySQL, transformed, and stored in Google Cloud Storage (GCS), before being loaded into BigQuery. A check is performed to ensure data integrity by comparing record counts between MySQL and BigQuery.

## Requirements

- PySpark
- MySQL
- Google Cloud Storage (GCS)
- Google BigQuery
- Google Cloud Service Account JSON Key

## How to Run

1. **Set up environment**:
    - Set your Google Cloud credentials:
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
    ```

2. **Run the PySpark job**:
    ```bash
    spark-submit mysql_to_bigquery_migration.py
    ```

3. **Check migration**:
    - The script compares the record counts between MySQL and BigQuery to ensure data integrity.

## Code Summary

- The script reads data from a MySQL database.
- The data is transformed and written to GCS in CSV format.
- The transformed data is loaded into a BigQuery table.
- Finally, the script compares the number of records between MySQL and BigQuery to verify that the migration was successful.

## Output

- If the migration is successful:
    ```
    Data Migration Successful: MySQL and BigQuery have the same record count.
    ```

- If there is a discrepancy:
    ```
    Data Migration Warning: Record mismatch detected!
    ```