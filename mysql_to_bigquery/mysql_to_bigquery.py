from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col
import os

# Set the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/syedmanzoor/Downloads/zomatoeda-fdf0eb944938.json'

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
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-fdf0eb944938.json") \
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

# # Cast 'delivery' and 'takeaway' columns from BOOLEAN to STRING if they are BOOLEAN types
# zomato_restaurants_india = zomato_restaurants_india.withColumn("delivery", col("delivery").cast("string"))
# zomato_restaurants_india = zomato_restaurants_india.withColumn("takeaway", col("takeaway").cast("string"))

# # Ensure 'average_cost_for_two' is numeric, fill invalid values with 0
# zomato_restaurants_india = zomato_restaurants_india.withColumn(
#     "average_cost_for_two",
#     when(col("average_cost_for_two").cast("int").isNull(), 0).otherwise(col("average_cost_for_two").cast("int")))

# Specify your BigQuery dataset and table
bigquery_table = "zomatoeda.zomato_dataset.zomato_india"

# Write the DataFrame to BigQuery
zomato_restaurants_india.write \
    .format("bigquery") \
    .option("table", bigquery_table) \
    .option("temporaryGcsBucket", "my-temporary-zomato-bucket") \
    .mode("overwrite") \
    .save()

print("Data written to BigQuery successfully!")


# count = zomato_restaurants_india.count()
# print(count)

# # storing the raw extracted data in gcs.
# zomato_restaurants_india.write.mode("overwrite").csv("gs://my-temporary-zomato-bucket/extracted_data")

# # # Select only relevant columns
# zomato_restaurants = zomato_restaurants_india.select(
#      "res_id", "name", "establishment", "address", "city", "locality", "cuisines", "timings", "rating_text")

# # # Rename columns
# zomato_restaurants = zomato_restaurants \
#      .withColumnRenamed("res_id", "Restaurant_ID") \
#     .withColumnRenamed("name", "Name") \
#     .withColumnRenamed("establishment", "Establishment") \
#     .withColumnRenamed("address", "Address") \
#     .withColumnRenamed("city", "City") \
#     .withColumnRenamed("locality", "Locality") \
#     .withColumnRenamed("cuisines", "Cuisines") \
#     .withColumnRenamed("timings", "Timing") \
#     .withColumnRenamed("rating_text", "Ratings")

# # # Map cities to their respective states
# zomato_restaurants = zomato_restaurants.withColumn(
#     "State",
#     when(zomato_restaurants["City"].isin("Bangalore", "Mysore", "Mangalore", "Udupi"), "Karnataka")
#     .when(zomato_restaurants["City"].isin("Chennai", "Coimbatore", "Madurai", "Trichy", "Salem", "Vellore", "Tirupati"), "Tamil Nadu")
#     .when(zomato_restaurants["City"].isin("Hyderabad", "Secunderabad"), "Telangana")
#     .when(zomato_restaurants["City"].isin("Guntur", "Vijayawada"), "Andhra Pradesh")
#     .when(zomato_restaurants["City"].isin("Mumbai", "Pune", "Nagpur", "Nashik", "Kolhapur", "Aurangabad"), "Maharashtra")
#     .when(zomato_restaurants["City"].isin("New Delhi", "Delhi"), "Delhi")
#     .when(zomato_restaurants["City"].isin("Gurgaon", "Faridabad"), "Haryana")
#     .when(zomato_restaurants["City"].isin("Noida", "Ghaziabad", "Greater Noida", "Allahabad", "Kanpur", "Lucknow", "Varanasi", "Gorakhpur"), "Uttar Pradesh")
#     .when(zomato_restaurants["City"].isin("Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"), "Rajasthan")
#     .when(zomato_restaurants["City"].isin("Kolkata", "Howrah", "Siliguri", "Kharagpur"), "West Bengal")
#     .when(zomato_restaurants["City"].isin("Chandigarh", "Mohali", "Panchkula"), "Chandigarh")
#     .when(zomato_restaurants["City"].isin("Patna", "Gaya"), "Bihar")
#     .when(zomato_restaurants["City"].isin("Goa", "Panaji"), "Goa")
#     .when(zomato_restaurants["City"].isin("Bhopal", "Indore", "Gwalior", "Jabalpur"), "Madhya Pradesh")
#     .when(zomato_restaurants["City"].isin("Bhubaneswar", "Cuttack"), "Odisha")
#     .when(zomato_restaurants["City"].isin("Shimla", "Manali", "Dharamshala"), "Himachal Pradesh")
#     .when(zomato_restaurants["City"].isin("Gangtok"), "Sikkim")
#     .when(zomato_restaurants["City"].isin("Jammu", "Srinagar"), "Jammu and Kashmir")
#     .when(zomato_restaurants["City"].isin("Amritsar", "Jalandhar", "Ludhiana", "Patiala"), "Punjab")
#     .when(zomato_restaurants["City"].isin("Guwahati"), "Assam")
#     .when(zomato_restaurants["City"].isin("Puducherry"), "Puducherry")
#     .when(zomato_restaurants["City"].isin("Trivandrum", "Thrissur", "Palakkad", "Kochi", "Alappuzha"), "Kerala")
#     .when(zomato_restaurants["City"].isin("Meerut", "Haridwar", "Rishikesh", "Dehradun", "Mussoorie", "Nainital"), "Uttarakhand")
#     .when(zomato_restaurants["City"].isin("Surat", "Ahmedabad", "Rajkot", "Gandhinagar", "Vadodara", "Junagadh", "Jamnagar"), "Gujarat")
#     .when(zomato_restaurants["City"].isin("Agra", "Mathura"), "Uttar Pradesh")
#     .when(zomato_restaurants["City"].isin("Darjeeling"), "West Bengal")
#     .when(zomato_restaurants["City"].isin("Pushkar", "Neemrana"), "Rajasthan")
#     .when(zomato_restaurants["City"].isin("Raipur"), "Chhattisgarh")
#     .when(zomato_restaurants["City"].isin("Siliguri"), "West Bengal")
#     .otherwise("Unknown"))  # Default for cities that don't have specific mapping yet

# # # storing transformed data to gcs
# zomato_restaurants.write.mode("overwrite").csv("gs://my-temporary-zomato-bucket/transformed_data")


# # # Get the distinct states in the data
# distinct_states = zomato_restaurants.select("State").distinct().collect()

# # # Loop through each state and save partitioned data to GCS
# for state_row in distinct_states:
#     state = state_row["State"]
    
#     # Format state name for GCS path
#     state_formatted = state.replace(' ', '_').lower()
    
#     # Write partitioned data for each state
#     zomato_restaurants.filter(zomato_restaurants["State"] == state) \
#         .write \
#         .partitionBy("State", "City") \
#         .mode("overwrite") \
#         .csv(f"gs://my-temporary-zomato-bucket/zomato_restaurants/{state_formatted}")
# print("Integration job completed")

