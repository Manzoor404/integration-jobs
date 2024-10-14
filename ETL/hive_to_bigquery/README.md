# Hive to BigQuery ETL

This project demonstrates an ETL process where data is extracted from a Hive table, transformed, and then loaded into Google Cloud Storage (GCS) and Google BigQuery. The transformations include cleaning, aggregating, and calculating growth rates for COVID-19 data.

## Steps Overview

1. **Extract**:
   - Data is read from a Hive table (`datamigration.covid_19_data`).

2. **Transform**:
   - The data is cleaned, including trimming dates and filtering out rows with 0 or null values.
   - Aggregations such as total confirmed cases, deaths, recovered, and active cases are calculated.
   - Daily growth rates for confirmed, deaths, and recovered cases are also calculated.

3. **Load**:
   - Transformed data is stored in GCS in CSV format.
   - Data is also written to Google BigQuery for further analysis and reporting.

## Running the ETL Job

1. **Set up environment**:
   - Make sure you have the required dependencies:
     - PySpark
     - Google Cloud Storage Connector
     - BigQuery Connector
   - Ensure Google Cloud credentials are configured:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
     ```

2. **Run the PySpark job**:
   ```bash
   spark-submit hive_to_bigquery_etl.py