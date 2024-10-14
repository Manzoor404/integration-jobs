# Hive to Redshift Data Migration

This project demonstrates migrating data from a Hive table to Amazon Redshift using PySpark. The script reads records from Hive and Redshift, compares the record counts, and verifies the success of the migration.

## Architecture

The pipeline follows this architecture:

![image](https://github.com/user-attachments/assets/07d0a3cc-e2c6-4fd8-a90b-a4ed09c59f48)


## Requirements

- PySpark
- Hive
- Amazon Redshift
- Redshift JDBC Driver

## How to Run

1. **Set up environment**:
    - Ensure that the Redshift JDBC driver is properly configured.
    - Update the Redshift connection details in the script (host, database, user, password).

2. **Run the PySpark job**:
    ```bash
    spark-submit hive_to_redshift_migration.py
    ```

3. **Check migration**:
    - The script compares the record counts between Hive and Redshift to ensure data integrity.

## Code Summary

- The script reads records from a Hive table and Redshift table.
- It compares the number of records in both Hive and Redshift.
- If the record counts match, the migration is considered successful.

## Output

- If the migration is successful:
    ```
    Data Migration Successful: Hive and Redshift have the same record count.
    ```

- If there is a discrepancy:
    ```
    Data Migration Warning: Record mismatch detected!
    ```
