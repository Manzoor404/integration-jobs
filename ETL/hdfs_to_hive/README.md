# Zomato ETL Project

This project demonstrates an ETL pipeline that reads raw data from HDFS, processes it by selecting and transforming specific columns, maps cities to their respective states, and writes the transformed data back to both HDFS and Hive.

## Steps in the ETL Process

1. **Read Raw Data from HDFS**:
    - The raw data is read from the HDFS path `hdfs://localhost:9000/raw_data/zomato/Zomato_restaurants_in_india.csv`.

2. **Load Raw Data into Hive**:
    - The raw data is written to the Hive table `Zomato.zomato_raw_table`.

3. **Store Raw Data Back to HDFS**:
    - The raw data is stored in the HDFS path `hdfs://localhost:9000/user/hive/warehouse/raw_data/zomato_raw_table`.

4. **Transform Data**:
    - Selected columns are renamed, and cities are mapped to their respective states.

5. **Store Transformed Data in HDFS**:
    - The transformed data is written to the HDFS path `hdfs://localhost:9000/user/hive/warehouse/transformed_data/zomato_transformed_table`.

6. **Load Transformed Data into Hive**:
    - The transformed data is written to the Hive table `Zomato.zomato_transformed_table`.

## How to Run the Project

1. **Ensure the environment is set up with Spark and Hive**.
2. **Run the ETL code**:
    ```bash
    spark-submit zomato_etl.scala
    ```

## Output

- The raw data is stored in both HDFS and Hive.
- The transformed data (with mapped states) is also stored in both HDFS and Hive.