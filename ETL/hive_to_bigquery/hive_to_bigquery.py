from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, trim, col, min, max, sum, lag
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Hive to BigQuery ETL") \
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

# Step 2: Save the raw DataFrame to GCS in CSV format
covid19_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/extracted_data/covid_data")
print("Raw Data Stored in GCS Bucket")

# Step 3: Clean up the ObservationDate column
covid19_df = covid19_df.withColumn("ObservationDate", to_date(trim(col("ObservationDate")), "MM/dd/yyyy"))

# Step 4: Find the least (earliest) date
earliest_date_df = covid19_df.agg(min(col("ObservationDate")).alias("earliest_date"))
earliest_date = earliest_date_df.collect()[0]["earliest_date"]
print(f"Earliest date: {earliest_date}")

# Step 5: Find the most recent (max) date
max_date_df = covid19_df.agg(max(col("ObservationDate")).alias("max_date"))
max_date = max_date_df.collect()[0]["max_date"]
print(f"Max date: {max_date}")

# Step 6: Count the total number of distinct days in the dataset
total_no_of_days = covid19_df.select("ObservationDate").distinct().count()
print(f"Total number of distinct days: {total_no_of_days}")

# Step 7: Filter out rows where Confirmed, Deaths, or Recovered are 0 or null
covid19_df = covid19_df.filter(
    (col("Confirmed") > 0) & (col("Confirmed").isNotNull()) &
    (col("Deaths") > 0) & (col("Deaths").isNotNull()) &
    (col("Recovered") > 0) & (col("Recovered").isNotNull())
)

# Step 8: Group data by ObservationDate for total confirmed, deaths, and recovered cases
total_confirmed_cases = covid19_df.groupBy("ObservationDate").agg(sum("Confirmed").alias("total_confirmed_cases"))
total_deaths_cases = covid19_df.groupBy("ObservationDate").agg(sum("Deaths").alias("total_deaths_cases"))
total_recovered_cases = covid19_df.groupBy("ObservationDate").agg(sum("Recovered").alias("total_recovered_cases"))

# Step 9: Join the three DataFrames (confirmed, deaths, recovered)
total_cases_by_date = total_confirmed_cases.join(total_deaths_cases, "ObservationDate", "inner")
total_cases_by_date = total_cases_by_date.join(total_recovered_cases, "ObservationDate", "inner")
total_cases_by_date.show()

# Step 10: Calculate active cases
total_active_cases = total_cases_by_date.withColumn(
    "active_cases", col("total_confirmed_cases") - col("total_deaths_cases") - col("total_recovered_cases")
)

# Step 11: Calculate daily growth rates for confirmed, deaths, and recovered cases
window_spec = Window.orderBy("ObservationDate")

total_active_cases = total_active_cases.withColumn(
    "growth_rate_confirmed", 
    ((col("total_confirmed_cases") - lag("total_confirmed_cases", 1).over(window_spec)) / lag("total_confirmed_cases", 1).over(window_spec) * 100))

total_active_cases = total_active_cases.withColumn(
    "growth_rate_deaths", 
    ((col("total_deaths_cases") - lag("total_deaths_cases", 1).over(window_spec)) / lag("total_deaths_cases", 1).over(window_spec) * 100))

total_active_cases = total_active_cases.withColumn(
    "growth_rate_recovered", 
    ((col("total_recovered_cases") - lag("total_recovered_cases", 1).over(window_spec)) / lag("total_recovered_cases", 1).over(window_spec) * 100))

total_active_cases.show()

# Step 12: Calculate daily new confirmed cases worldwide
total_active_cases = total_active_cases.withColumn(
    "daily_new_confirmed_cases", 
    col("total_confirmed_cases") - lag("total_confirmed_cases", 1).over(window_spec)
)

# Show the DataFrame with daily new confirmed cases
daily_new_confirmed_cases = total_active_cases.select("ObservationDate", "total_confirmed_cases", "daily_new_confirmed_cases")
daily_new_confirmed_cases.show()

# Step 13: Filter the data for the most recent date (max_date)
latest_data = covid19_df.filter(col("ObservationDate") == max_date)

# Step 14: Aggregate by Country/Region for total confirmed, deaths, recovered, and active cases
country_level_data = latest_data.groupBy("Country_Region").agg(
    sum("Confirmed").alias("total_confirmed_cases"),
    sum("Deaths").alias("total_deaths_cases"),
    sum("Recovered").alias("total_recovered_cases"),
    (sum("Confirmed") - sum("Deaths") - sum("Recovered")).alias("active_cases")
)

country_level_data.show()

# Step 15: Saving the total cases by date to GCS
total_cases_by_date.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/total_cases_by_date")
print("total_cases_by_date data stored in bucket")

# Saving the total active cases with growth rates to GCS
total_active_cases.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/total_active_cases_with_growth_rates")
print("total_active_cases data stored in gcs bucket")

# Saving the daily new confirmed cases to GCS
daily_new_confirmed_cases.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/daily_new_confirmed_cases")
print("daily_new_confirmed_cases data stored in gcs bucket")

# Saving the country-level aggregated data to GCS
country_level_data.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/country_level_data")
print("country_level_data data stored in gcs bucket")


# Writing the data in big query
print("Writing all required data in BigQuery")

# Saving the total active cases with growth rates to BigQuery
total_active_cases.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.total_active_cases_with_growth_rates") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("total_active_cases data stored in bigquery")

# Saving the daily new confirmed cases to BigQuery
daily_new_confirmed_cases.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.daily_new_confirmed_cases") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("daily_new_confirmed_cases data stored in bigquery")



# Saving the country-level aggregated data to BigQuery
country_level_data.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.country_level_data") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("country_level_data data stored in bigquery")

print("ETL Job Completed")












# # Daily overall report
# daily_overall_report = covid19_df.select("ObservationDate")

# # Step : Load data from GCS into BigQuery
# bigquery_table = "zomatoeda-435904.covid_19_dataset.covid_data"

# covid19_df.write \
#     .format("bigquery") \
#     .option("table", bigquery_table) \
#     .option("parentProject", "zomatoeda-435904") \
#     .option("temporaryGcsBucket", "syedmanzoor-bucket") \
#     .mode("overwrite") \
#     .save()

# print("Data written to BigQuery successfully!")

# # Step 4: Read data back from BigQuery to check the record count
# bigquery_df = spark.read \
#     .format("bigquery") \
#     .option("table", bigquery_table) \
#     .load()

# total_records_bigquery = bigquery_df.count()
# print(f"Total records in BigQuery: {total_records_bigquery}")

# # Function to compare Hive and BigQuery record counts
# def records():
#     if total_records_hive == total_records_bigquery:
#         print(f"Data Migration Successful: Hive and BigQuery have the same record count of {total_records_hive} records. The data integrity is maintained.")
#     else:
#         print(f"Data Migration Warning: Record mismatch detected! Hive has {total_records_hive} records, but BigQuery contains {total_records_bigquery} records. Please review the migration process for discrepancies.")

# # Call the function to check data migration success
# records()