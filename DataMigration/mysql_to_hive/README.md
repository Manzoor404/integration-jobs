# MySQL to Hive Data Migration

This project demonstrates how to migrate data from a MySQL table to a Hive table using PySpark. The script extracts data from MySQL, loads it into Hive, and compares record counts to ensure data integrity.

## Requirements

- PySpark
- MySQL
- Hive
- MySQL JDBC Driver

## How to Run

1. **Set up environment**:
    - Ensure the MySQL JDBC driver is properly configured.
    - Update MySQL connection details (URL, user, password) in the script.

2. **Run the PySpark job**:
    ```bash
    spark-submit mysql_to_hive_migration.py
    ```

3. **Check migration**:
    - The script compares record counts between MySQL and Hive to verify the migration.

## Code Summary

- The script extracts data from a MySQL database using JDBC.
- It writes the extracted data into a Hive table.
- The script then compares the record counts between MySQL and Hive to ensure successful migration.

## Output

- If the migration is successful:
    ```
    Data Migration Successful: MySQL and Hive have the same record count.
    ```

- If there is a discrepancy:
    ```
    Data Migration Warning: Record mismatch detected!
    ```