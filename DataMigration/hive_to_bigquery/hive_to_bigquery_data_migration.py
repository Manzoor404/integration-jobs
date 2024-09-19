from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Hive to BigQuery Data Migration") \
    .config("spark.jars", 
            "/Users/syedmanzoor/Downloads/gcs-connector-hadoop2-2.2.5-shaded.jar,"
            "/Users/syedmanzoor/Downloads/google-oauth-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-api-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-http-client-1.39.2.jar,"
            "/Users/syedmanzoor/Downloads/google-auth-library-oauth2-http-0.20.0.jar,"
            "/Users/syedmanzoor/Downloads/spark-bigquery-with-dependencies_2.12-0.30.0.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 1: Read data from Hive table using Spark
covid19_df = spark.read.table("datamigration.covid_19_data")
total_records_hive = covid19_df.count()
print(f"Total records in Hive: {total_records_hive}")

# Step 2: Save DataFrame to GCS in CSV format
covid19_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/covid19/covid_data")

print("Data stored in GCS")

# Step 3: Load data from GCS into BigQuery
bigquery_table = "zomatoeda-435904.covid_19_dataset.covid_data"

covid19_df.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()

print("Data written to BigQuery successfully!")

# Step 4: Read data back from BigQuery to check the record count
bigquery_df = spark.read \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .load()

total_records_bigquery = bigquery_df.count()
print(f"Total records in BigQuery: {total_records_bigquery}")

# Function to compare Hive and BigQuery record counts
def data_migration():
    if total_records_hive == total_records_bigquery:
        print(f"Data Migration Successful: Hive and BigQuery have the same record count of {total_records_hive} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! Hive has {total_records_hive} records, but BigQuery contains {total_records_bigquery} records. Please review the migration process for discrepancies.")

# Call the function to check data migration success
data_migration()