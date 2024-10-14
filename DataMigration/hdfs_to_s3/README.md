# HDFS to S3 Data Migration

This project demonstrates migrating data from Hadoop HDFS to Amazon S3 using PySpark. The data is read from HDFS, processed, and stored in an S3 bucket. A comparison is done to ensure data integrity between HDFS and S3.

## Architecture Diagram
![image](https://github.com/user-attachments/assets/12e328f7-6303-4716-926c-a50949d7427e)


## Requirements

- PySpark
- Hadoop HDFS
- Amazon S3
- AWS SDK

## How to Run

1. **Set up the environment**:
    - Make sure you have the necessary dependencies and set up AWS credentials.

2. **Run the PySpark job**:
    ```bash
    spark-submit hdfs_to_s3_migration.py
    ```

3. **Check migration**:
    - The script compares the record counts between HDFS and S3 to ensure the migration's success.

## Code Summary

- The script reads data from HDFS and writes it to an S3 bucket.
- It compares the number of records in both HDFS and S3 to verify successful data migration.

## Output

- If the migration is successful:
    ```
    Data Migration Successful: HDFS and S3 have the same record count.
    ```

- If there's a discrepancy:
    ```
    Data Migration Warning: Record mismatch detected!
    ```
