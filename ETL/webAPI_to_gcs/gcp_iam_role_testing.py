# from google.cloud import storage

# client = storage.Client.from_service_account_json('/Users/syedmanzoor/Downloads/zomatoeda-fdf0eb944938.json')
# bucket = client.get_bucket('my-temporary-zomato-bucket')
# blobs = bucket.list_blobs()
# for blob in blobs:
#     print(blob.name)

from pyspark.sql import SparkSession
import os

# Set the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/syedmanzoor/Downloads/zomatoeda-fdf0eb944938.json'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GCS Test") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/gcs-connector-hadoop2-2.2.5-shaded.jar,"
                      "/Users/syedmanzoor/Downloads/google-oauth-client-1.31.5.jar,"
                      "/Users/syedmanzoor/Downloads/google-api-client-1.31.5.jar,"
                      "/Users/syedmanzoor/Downloads/google-http-client-1.39.2.jar,"
                      "/Users/syedmanzoor/Downloads/google-auth-library-oauth2-http-0.20.0.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/syedmanzoor/Downloads/zomatoeda-fdf0eb944938.json") \
    .getOrCreate()

# Create a small DataFrame
data = [("1", "Test")]
df = spark.createDataFrame(data, ["id", "value"])

# Write DataFrame to GCS
df.write.mode("overwrite").csv("gs://my-temporary-zomato-bucket/test_output")

print("Data successfully written to GCS")
# print("printing jars")
# print(spark.sparkContext.getConf().getAll())
# print("run completed")
# Write DataFrame to GCS
#df.write.csv("gs://my-temporary-zomato-bucket/test_output")