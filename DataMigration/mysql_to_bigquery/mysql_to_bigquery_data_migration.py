from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col
import os

# Set the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to BigQuery") \
    .config("spark.jars", 
            "/Users/syedmanzoor/Downloads/gcs-connector-hadoop2-2.2.5-shaded.jar,"
            "/Users/syedmanzoor/Downloads/google-oauth-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-api-client-1.31.5.jar,"
            "/Users/syedmanzoor/Downloads/google-http-client-1.39.2.jar,"
            "/Users/syedmanzoor/Downloads/google-auth-library-oauth2-http-0.20.0.jar,"
            "/Users/syedmanzoor/Downloads/mysql-connector-java-8.0.30.jar,"
            "/Users/syedmanzoor/Downloads/spark-bigquery-with-dependencies_2.12-0.30.0.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-435904-64660feb251c.json") \
    .getOrCreate()

# JDBC URL for MySQL
jdbc_url = "jdbc:mysql://localhost:3306/zomato_db"

# MySQL connection properties
connection_properties = {
    "user": "root",
    "password": "$yed7007",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load the MySQL table into a PySpark DataFrame
zomato_restaurants_india = spark.read.jdbc(url=jdbc_url, table="zomato_restaurants", properties=connection_properties)

# Get record count from MySQL
total_records_mysql = zomato_restaurants_india.count()

# storing the raw extracted data into gcs.
zomato_restaurants_india.write.mode("overwrite").csv("gs://syedmanzoor-bucket/extracted_data")
print("raw_data stored in gcs syedmanzoor-bucket/extracted_data")

# Select only required columns
zomato_restaurants = zomato_restaurants_india.select(
     "res_id", "name", "establishment", "address", "city", "locality", "cuisines", "timings", "rating_text")

# Rename columns
zomato_restaurants = zomato_restaurants \
     .withColumnRenamed("res_id", "Restaurant_ID") \
    .withColumnRenamed("name", "Name") \
    .withColumnRenamed("establishment", "Establishment") \
    .withColumnRenamed("address", "Address") \
    .withColumnRenamed("city", "City") \
    .withColumnRenamed("locality", "Locality") \
    .withColumnRenamed("cuisines", "Cuisines") \
    .withColumnRenamed("timings", "Timing") \
    .withColumnRenamed("rating_text", "Ratings")

# Map cities to their respective states
zomato_restaurants = zomato_restaurants.withColumn(
    "State",
    when(zomato_restaurants["City"].isin("Bangalore", "Mysore", "Mangalore", "Udupi"), "Karnataka")
    .when(zomato_restaurants["City"].isin("Chennai", "Coimbatore", "Madurai", "Trichy", "Salem", "Vellore"), "Tamil Nadu")
    .when(zomato_restaurants["City"].isin("Hyderabad", "Secunderabad"), "Telangana")
    .when(zomato_restaurants["City"].isin("Guntur", "Vijayawada", "Tirupati"), "Andhra Pradesh")
    .when(zomato_restaurants["City"].isin("Mumbai", "Pune", "Nagpur", "Nashik", "Kolhapur", "Aurangabad"), "Maharashtra")
    .when(zomato_restaurants["City"].isin("New Delhi", "Delhi"), "Delhi")
    .when(zomato_restaurants["City"].isin("Gurgaon", "Faridabad"), "Haryana")
    .when(zomato_restaurants["City"].isin("Noida", "Ghaziabad", "Greater Noida", "Allahabad", "Kanpur", "Lucknow", "Varanasi", "Gorakhpur"), "Uttar Pradesh")
    .when(zomato_restaurants["City"].isin("Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"), "Rajasthan")
    .when(zomato_restaurants["City"].isin("Kolkata", "Howrah", "Siliguri", "Kharagpur"), "West Bengal")
    .when(zomato_restaurants["City"].isin("Chandigarh", "Mohali", "Panchkula"), "Chandigarh")
    .when(zomato_restaurants["City"].isin("Patna", "Gaya"), "Bihar")
    .when(zomato_restaurants["City"].isin("Goa", "Panaji"), "Goa")
    .when(zomato_restaurants["City"].isin("Bhopal", "Indore", "Gwalior", "Jabalpur"), "Madhya Pradesh")
    .when(zomato_restaurants["City"].isin("Bhubaneswar", "Cuttack"), "Odisha")
    .when(zomato_restaurants["City"].isin("Shimla", "Manali", "Dharamshala"), "Himachal Pradesh")
    .when(zomato_restaurants["City"].isin("Gangtok"), "Sikkim")
    .when(zomato_restaurants["City"].isin("Jammu", "Srinagar"), "Jammu and Kashmir")
    .when(zomato_restaurants["City"].isin("Amritsar", "Jalandhar", "Ludhiana", "Patiala"), "Punjab")
    .when(zomato_restaurants["City"].isin("Guwahati"), "Assam")
    .when(zomato_restaurants["City"].isin("Puducherry"), "Puducherry")
    .when(zomato_restaurants["City"].isin("Trivandrum", "Thrissur", "Palakkad", "Kochi", "Alappuzha"), "Kerala")
    .when(zomato_restaurants["City"].isin("Meerut", "Haridwar", "Rishikesh", "Dehradun", "Mussoorie", "Nainital"), "Uttarakhand")
    .when(zomato_restaurants["City"].isin("Surat", "Ahmedabad", "Rajkot", "Gandhinagar", "Vadodara", "Junagadh", "Jamnagar"), "Gujarat")
    .when(zomato_restaurants["City"].isin("Agra", "Mathura"), "Uttar Pradesh")
    .when(zomato_restaurants["City"].isin("Darjeeling"), "West Bengal")
    .when(zomato_restaurants["City"].isin("Pushkar", "Neemrana"), "Rajasthan")
    .when(zomato_restaurants["City"].isin("Raipur"), "Chhattisgarh")
    .when(zomato_restaurants["City"].isin("Siliguri"), "West Bengal")
    .otherwise("Unknown"))  # Default for cities that don't have specific mapping yet

# storing transformed data to gcs
zomato_restaurants.write.mode("overwrite").csv("gs://syedmanzoor-bucket/transformed_data")
print("transformed_data stored in gcs syedmanzoor-bucket/transformed_data")

# Specify your BigQuery dataset and table
bigquery_table = "zomatoeda-435904.zomato_new_dataset.zomato_india"

# Write the DataFrame to BigQuery
zomato_restaurants_india.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()

# Read data back from BigQuery to check the record count
bigquery_data = spark.read \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .load()

# Get record count from BigQuery
total_records_bigquery = bigquery_data.count()

# Function to compare MySQL and BigQuery record counts
def data_migration():
    if total_records_mysql == total_records_bigquery:
        print(f"Data Migration Successful: MySQL and BigQuery have the same record count of {total_records_mysql} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! MySQL has {total_records_mysql} records, but BigQuery contains {total_records_bigquery} records. Please review the migration process for discrepancies.")

# Call the function to check data migration success
data_migration()

print("Data written to BigQuery successfully!")