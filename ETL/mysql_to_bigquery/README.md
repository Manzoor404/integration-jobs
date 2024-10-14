# MySQL to BigQuery Data Transformation

This project demonstrates extracting restaurant data from a MySQL database, transforming it by mapping cities to their respective states, and storing the transformed data into Google Cloud Storage (GCS). The data is partitioned by state and city and stored in GCS using PySpark.

## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/49ab5464-38fd-42ff-bb38-8116e426558d)


## Requirements

- PySpark
- MySQL
- Google Cloud Storage (GCS)
- Google Cloud Service Account JSON key

## How to Run

1. **Set up the environment**:
    - Configure Google Cloud credentials:
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
    ```

2. **Run the PySpark job**:
    ```bash
    spark-submit mysql_to_bigquery.py
    ```

## Code Summary

- The script extracts data from a MySQL table into a PySpark DataFrame.
- It selects and renames specific columns.
- Cities are mapped to their respective states.
- The transformed data is stored in GCS.
- Data is further partitioned by state and city before being stored in GCS.

## Output

- Extracted data stored in GCS in the `extracted_data` folder.
- Transformed and partitioned data stored in GCS in the `zomato_restaurants` folder, partitioned by state and city.
