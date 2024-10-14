![image](https://github.com/user-attachments/assets/a4191b75-7b9b-4d2f-9700-dade9196b8dc)# MySQL to Redshift Data Migration

This project demonstrates how to migrate data from MySQL to Amazon Redshift using PySpark. The script loads data from a MySQL table, saves it temporarily in an S3 bucket, and finally writes the data to an Amazon Redshift table.

## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/71a15fbf-a502-475a-b185-80b56dae3e0b)


## Requirements

- PySpark
- MySQL
- Amazon S3
- Amazon Redshift
- Required JDBC drivers for MySQL and Redshift

## How to Run

1. **Set up environment**:
    - Ensure that the necessary JDBC drivers (MySQL and Redshift) are included.
    - Update MySQL and Redshift connection details in the script.

2. **Run the PySpark job**:
    ```bash
    spark-submit mysql_to_redshift_migration.py
    ```

3. **Process Overview**:
    - The script reads a filtered dataset from MySQL (for India).
    - It temporarily stores the data in an S3 bucket.
    - Finally, the data is written to an Amazon Redshift table.

## Output

- The script will display the following messages:
    ```
    Test data stored to S3 bucket
    Data loaded to Redshift
    ```
