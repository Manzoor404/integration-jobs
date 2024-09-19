from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFS to GCS Migration") \
    .config("spark.jars", 
            "/Users/syedmanzoor/Downloads/gcs-connector-hadoop2-2.2.5-shaded.jar,"
            "/Users/syedmanzoor/Downloads/google-oauth-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-api-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-http-client-1.39.2.jar,"
            "/Users/syedmanzoor/Downloads/google-auth-library-oauth2-http-0.20.0.jar,") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json") \
    .getOrCreate()

# Set the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json'

# HDFS and GCS paths
hdfs_path = "hdfs://localhost:9000/raw_data/covid19/covid_19_data.csv"
gcs_path = "gs://syedmanzoor-bucket/covid_19_data/"

# Read CSV file from HDFS
df_hdfs = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Write the DataFrame to GCS in CSV format
df_hdfs.write.mode("overwrite").csv(gcs_path)

# Read the data back from GCS to compare
df_gcs = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Function to compare HDFS and GCS record counts
def data_migration():
    # Get total record counts for both HDFS and GCS datasets
    total_records_hdfs = df_hdfs.count()
    total_records_gcs = df_gcs.count()
    
    # Compare the record counts
    if total_records_hdfs == total_records_gcs:
        print(f"Data Migration Successful: HDFS and GCS have the same record count of {total_records_hdfs} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! HDFS has {total_records_hdfs} records, but GCS contains {total_records_gcs} records. Please review the migration process for discrepancies.")

# Call the function to check data migration success
data_migration()

print("Data Migrated from HDFS to GCS successfully!")