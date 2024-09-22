from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, trim, col, min, max, sum, lag
from pyspark.sql.window import Window

# Initialize the Spark session on DataProc
spark = SparkSession.builder \
    .appName("DataProc ETL from GCS to BigQuery") \
    .getOrCreate()

# Step 1: Read data from GCS CSV file
gcs_path = "gs://syedmanzoor-bucket/covid19/covid_data/part-00000-dc0d905c-2467-4bf6-9b2d-7796da91aacb-c000.csv"
covid19_df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Step 2: Clean up the ObservationDate column
covid19_df = covid19_df.withColumn("ObservationDate", to_date(trim(col("ObservationDate")), "MM/dd/yyyy"))

# Step 3: Find the least (earliest) date
earliest_date_df = covid19_df.agg(min(col("ObservationDate")).alias("earliest_date"))
earliest_date = earliest_date_df.collect()[0]["earliest_date"]
print(f"Earliest date: {earliest_date}")

# Step 4: Find the most recent (max) date
max_date_df = covid19_df.agg(max(col("ObservationDate")).alias("max_date"))
max_date = max_date_df.collect()[0]["max_date"]
print(f"Max date: {max_date}")

# Step 5: Count the total number of distinct days in the dataset
total_no_of_days = covid19_df.select("ObservationDate").distinct().count()
print(f"Total number of distinct days: {total_no_of_days}")

# Step 6: Filter out rows where Confirmed, Deaths, or Recovered are 0 or null
covid19_df = covid19_df.filter(
    (col("Confirmed") > 0) & (col("Confirmed").isNotNull()) &
    (col("Deaths") > 0) & (col("Deaths").isNotNull()) &
    (col("Recovered") > 0) & (col("Recovered").isNotNull())
)

# Step 7: Group data by ObservationDate for total confirmed, deaths, and recovered cases
total_confirmed_cases = covid19_df.groupBy("ObservationDate").agg(sum("Confirmed").alias("total_confirmed_cases"))
total_deaths_cases = covid19_df.groupBy("ObservationDate").agg(sum("Deaths").alias("total_deaths_cases"))
total_recovered_cases = covid19_df.groupBy("ObservationDate").agg(sum("Recovered").alias("total_recovered_cases"))

# Step 8: Join the three DataFrames (confirmed, deaths, recovered)
total_cases_by_date = total_confirmed_cases.join(total_deaths_cases, "ObservationDate", "inner")
total_cases_by_date = total_cases_by_date.join(total_recovered_cases, "ObservationDate", "inner")
total_cases_by_date.show()

# Step 9: Calculate active cases
total_active_cases = total_cases_by_date.withColumn(
    "active_cases", col("total_confirmed_cases") - col("total_deaths_cases") - col("total_recovered_cases")
)

# Step 10: Calculate daily growth rates for confirmed, deaths, and recovered cases
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

# Step 11: Calculate daily new confirmed cases worldwide
total_active_cases = total_active_cases.withColumn(
    "daily_new_confirmed_cases", 
    col("total_confirmed_cases") - lag("total_confirmed_cases", 1).over(window_spec)
)

# Show the DataFrame with daily new confirmed cases
daily_new_confirmed_cases = total_active_cases.select("ObservationDate", "total_confirmed_cases", "daily_new_confirmed_cases")
daily_new_confirmed_cases.show()

# Step 12: Filter the data for the most recent date (max_date)
latest_data = covid19_df.filter(col("ObservationDate") == max_date)

# Step 13: Aggregate by Country/Region for total confirmed, deaths, recovered, and active cases
country_level_data = latest_data.groupBy("Country_Region").agg(
    sum("Confirmed").alias("total_confirmed_cases"),
    sum("Deaths").alias("total_deaths_cases"),
    sum("Recovered").alias("total_recovered_cases"),
    (sum("Confirmed") - sum("Deaths") - sum("Recovered")).alias("active_cases")
)

country_level_data.show()

# Step 14: Save the total cases by date to GCS
total_cases_by_date.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/total_cases_by_date")
print("total_cases_by_date data stored in GCS")

# Step 15: Save the total active cases with growth rates to GCS
total_active_cases.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/total_active_cases_with_growth_rates")
print("total_active_cases data stored in GCS")

# Step 16: Save the daily new confirmed cases to GCS
daily_new_confirmed_cases.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/daily_new_confirmed_cases")
print("daily_new_confirmed_cases data stored in GCS")

# Step 17: Save the country-level aggregated data to GCS
country_level_data.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://syedmanzoor-bucket/transformed_data/country_level_data")
print("country_level_data data stored in GCS")

# Writing the data in BigQuery
print("Writing all required data in BigQuery")

# Step 18: Save the total active cases with growth rates to BigQuery
total_active_cases.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.total_active_cases_with_growth_rates") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("total_active_cases data stored in BigQuery")

# Step 19: Save the daily new confirmed cases to BigQuery
daily_new_confirmed_cases.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.daily_new_confirmed_cases") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("daily_new_confirmed_cases data stored in BigQuery")

# Step 20: Save the country-level aggregated data to BigQuery
country_level_data.write \
    .format("bigquery") \
    .option("table", "covid_19_dataset.country_level_data") \
    .option("parentProject", "zomatoeda-435904") \
    .option("temporaryGcsBucket", "syedmanzoor-bucket") \
    .mode("overwrite") \
    .save()
print("country_level_data data stored in BigQuery")

print("ETL Job Completed")