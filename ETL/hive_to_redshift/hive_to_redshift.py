from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
print("Initializing Spark session...")
spark = SparkSession.builder \
    .appName("Cars India ETL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/hadoop-aws-3.3.1.jar,"
                          "/Users/syedmanzoor/Downloads/aws-java-sdk-bundle-1.11.901.jar,"
                          "/Users/syedmanzoor/Downloads/redshift-jdbc42-2.1.0.30/redshift-jdbc42-2.1.0.30.jar") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark session initialized successfully.")

# Define the Hive table
hive_table = "cars_india.cars_india"

# Load the data from Hive into a DataFrame
cars_india = spark.read.table(hive_table)

s3_extracted_data_path = "s3a://syedmanzoor/staging_data/extracted_data/cars_india/"
s3_transformed_data_path = "s3a://syedmanzoor/final_data/transformed_data/cars_india/"

# Redshift connection details
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439
redshift_raw_table = "cars_india_raw_table"
redshift_final_table = "cars_india_final_table"

# Redshift JDBC connection URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

# Write the filtered and data to S3
cars_india.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(s3_extracted_data_path)

print("Raw Data Stored in S3.")

# Filter out the first row where 'brand' is equal to 'brand' (header row)
cars_india = cars_india.filter(cars_india['model'] != 'model')

# Convert BOOLEAN columns to INTEGER (0 or 1)
cars_india = cars_india.withColumn("has_insurance", col("has_insurance").cast("int")) \
                       .withColumn("spare_key", col("spare_key").cast("int"))


# Write the DataFrame to Redshift
cars_india.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_raw_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()

print("Raw Data loaded to Redshift")

# Register DataFrame as a SQL temporary view
cars_india.createOrReplaceTempView("cars_india_view")

state_registration_query = """
                    SELECT 
                        brand,
                        model,
                        transmission,
                        make_year,
                        reg_year,
                        fuel_type,
                        engine_capacity,
                        km_driven,
                        ownership,
                        price,
                        overall_cost,
                        has_insurance,
                        spare_key,
                        reg_number,
                        SUBSTRING(reg_number, 1, 2) AS state,  
                        title
                    FROM 
                        cars_india_view"""

cars_india = spark.sql(state_registration_query)

# Write partitioned data into S3
cars_india.write.partitionBy('brand', 'fuel_type', 'transmission', 'state') \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(s3_transformed_data_path)

print("Transformed Data Loaded to S3.")

# Write the transformed df to Redshift
cars_india.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_final_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()
print("Transformed Data loaded in Redshift Final Table")
print("ETL Job Completed.")