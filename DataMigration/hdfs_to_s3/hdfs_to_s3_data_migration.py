from pyspark.sql import SparkSession

# Initialize Spark session
print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("HDFS to S3 Migration") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/hadoop-aws-3.3.1.jar,/Users/syedmanzoor/Downloads/aws-java-sdk-bundle-1.11.901.jar") \
    .getOrCreate()

print("Spark session initialized successfully.")

# HDFS paths
hdfs_path = "hdfs://localhost:9000/raw_data/zomato/Zomato_restaurants_in_india.csv"
s3_path = "s3a://syedmanzoor/raw_data/zomato/"

# Reading data from HDFS
print(f"Reading data from HDFS path: {hdfs_path}")
covid19_data = spark.read.csv(hdfs_path, header=True, inferSchema=True)
hdfs_records = covid19_data.count()
print(f"Data read from HDFS successfully with {hdfs_records} records.")

# Save the data to S3
print(f"Writing data to S3 bucket at path: {s3_path}")
covid19_data.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(s3_path)
print(f"Data successfully written to S3 bucket.")

# Read the data back from S3
print(f"Reading back the data from S3 bucket at path: {s3_path}")
s3_data = spark.read.csv(s3_path, header=True, inferSchema=True)
s3_records = s3_data.count()
print(f"Data successfully read from S3 with {s3_records} records.")

# Function to compare HDFS and S3 record counts
def data_migration():
    print("Comparing HDFS and S3 record counts...")
    
    # Compare the record counts
    if hdfs_records == s3_records:
        print(f"Data Migration Successful: HDFS and S3 have the same record count of {hdfs_records} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! HDFS has {hdfs_records} records, but S3 contains {s3_records} records. Please review the migration process for discrepancies.")

# Call the function to verify migration
data_migration()