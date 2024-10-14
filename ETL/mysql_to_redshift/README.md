# MySQL to Redshift ETL

This project demonstrates an ETL process where data is extracted from a MySQL database, processed using PySpark, and then loaded into Amazon Redshift. The script filters COVID-19 data for India and calculates global totals, saving the results to Amazon S3 before loading them into Redshift.

## Architecture

The pipeline follows this architecture:
![image](https://github.com/user-attachments/assets/99139cfa-392b-4fda-8bd2-747fe042228a)


## Requirements

- PySpark
- MySQL
- Amazon S3
- Amazon Redshift
- JDBC Drivers (MySQL and Redshift)

## How to Run

1. **Set up the environment**:
    - Install the necessary dependencies and ensure that the MySQL and Redshift JDBC drivers are available.
    - Update the MySQL and Redshift connection details in the script.

2. **Run the PySpark ETL job**:
    ```bash
    spark-submit mysql_to_redshift_etl.py
    ```

3. **Check output**:
    - Data is saved to Amazon S3 and loaded into Redshift tables for both India-specific and global COVID-19 cases.

## Code Summary

- **Extract**: Data is extracted from a MySQL table (`covid_19_data`), filtered for India and globally.
- **Transform**: The script calculates total confirmed, deaths, recovered, and active cases for each country and for India over time.
- **Load**: The results are stored in Amazon S3 and then loaded into Redshift tables.

## Output

- India-specific COVID-19 data is saved to Redshift in the table `covid19_india_table`.
- Global COVID-19 data is saved to Redshift in the table `covid19_global_table`.
- Both datasets are also stored as CSV files in an S3 bucket.
