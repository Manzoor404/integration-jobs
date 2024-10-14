# Cars India ETL Process

This project demonstrates an ETL (Extract, Transform, Load) process using PySpark. The data is extracted from Hive, transformed, and then loaded into Amazon S3 and Amazon Redshift.


## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/261b21c2-0fee-42bc-82d8-6d58b2eee804)


## Requirements

- PySpark
- Hive
- Amazon S3
- Amazon Redshift
- AWS SDK
- Redshift JDBC Driver

## Steps

1. **Extract**:
   - Data is read from a Hive table (`cars_india.cars_india`).
   - The raw data is written to S3 in CSV format.

2. **Transform**:
   - The header row is filtered out.
   - Boolean columns (`has_insurance`, `spare_key`) are converted to integer values (0 or 1).
   - The data is partitioned by `brand`, `fuel_type`, `transmission`, and `state`, and saved to S3.

3. **Load**:
   - The raw data is loaded into the Redshift raw table (`cars_india_raw_table`).
   - The transformed data is loaded into the Redshift final table (`cars_india_final_table`).

## How to Run

1. **Set up the environment**:
   - Ensure the necessary dependencies and jars are available (AWS SDK, Redshift JDBC Driver).
   - Update the script with the correct S3 and Redshift connection details.

2. **Run the PySpark job**:
   ```bash
   spark-submit cars_india_etl.py
