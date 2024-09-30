from pyspark.sql import SparkSession

# Initialize Spark session with necessary configurations and JARs
spark = SparkSession.builder \
    .appName("Hive to Redshift Migration") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/redshift-jdbc42-2.1.0.30/redshift-jdbc42-2.1.0.30.jar") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive table record count
hive_records = spark.read.table("cars_india.cars_india").count()
print(f"Total records in Hive: {hive_records}")

# Redshift connection details
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439
redshift_table = "cars_india"

# Redshift JDBC connection URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

# Redshift table record count
redshift_df = spark.read \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .load()

redshift_records = redshift_df.count()
print(f"Total records in Redshift: {redshift_records}")

# Function to compare Hive and Redshift record counts
def data_migration():
    if hive_records == redshift_records:
        print(f"Data Migration Successful: Hive and Redshift have the same record count of {hive_records} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! Hive has {hive_records} records, but Redshift contains {redshift_records} records. Please review the migration process for discrepancies.")

# Call the function to check data migration success
data_migration()