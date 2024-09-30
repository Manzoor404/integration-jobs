from pyspark.sql import SparkSession

# Initialize Spark session with the necessary JARs and resource configurations
spark = SparkSession.builder \
    .appName("MySQL to Redshift ETL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/mysql-connector-java-8.0.30.jar,"
                          "/Users/syedmanzoor/Downloads/hadoop-aws-3.3.1.jar,"
                          "/Users/syedmanzoor/Downloads/aws-java-sdk-bundle-1.11.901.jar,"
                          "/Users/syedmanzoor/Downloads/redshift-jdbc42-2.1.0.30/redshift-jdbc42-2.1.0.30.jar") \
    .getOrCreate()

# MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/covid19"
connection_properties = {
    "user": "root",
    "password": "$yed7007",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from MySQL into a Spark DataFrame (filtered by 'India')
query = "(SELECT * FROM covid_19_data WHERE country_region = 'India') AS temp"
covid19_global = "(SELECT * FROM covid_19_data) AS Covid19"
covid19_data = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
covid_global_data = spark.read.jdbc(url=jdbc_url, table=covid19_global, properties=connection_properties)

# Register the DataFrame as a temporary view for SQL queries
covid19_data.createOrReplaceTempView("temp")
covid_global_data.createOrReplaceTempView("Covid19")

total_cases_query = ("""
                        SELECT 
                            ObservationDate, 
                            SUM(Confirmed) AS Total_Confirmed, 
                            SUM(Deaths) AS Total_Deaths, 
                            SUM(Recovered) AS Total_Recovered, 
                            (SUM(Confirmed) - (SUM(Deaths) + SUM(Recovered))) AS Active_Cases
                        FROM temp 
                        GROUP BY ObservationDate 
                        ORDER BY ObservationDate
                    """)

# Query the registered view using Spark SQL to get total cases
total_cases = spark.sql(total_cases_query)

# Show the results
total_cases.show()

total_cases_by_country_query = ("""
                        SELECT 
                            Country_Region, 
                            SUM(Confirmed) AS Total_Confirmed, 
                            SUM(Deaths) AS Total_Deaths, 
                            SUM(Recovered) AS Total_Recovered, 
                            (SUM(Confirmed) - (SUM(Deaths) + SUM(Recovered))) AS Active_Cases
                        FROM Covid19 
                        GROUP BY Country_Region 
                        ORDER BY Country_Region
                    """)

total_cases_country = spark.sql(total_cases_by_country_query)
total_cases_country.show()


# Save the total_cases S3
total_cases.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3a://syedmanzoor/staging_data/covid19_India/covid_india")
print("total_cases data stored to S3 bucket")



# Save the total_cases_country in S3
total_cases_country.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3a://syedmanzoor/staging_data/covid19_India/covid_global")
print("total_cases_country data stored to S3 bucket")

# Parameters for the Redshift connection
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439

# Target Redshift table
redshift_global_table = "covid19_global_table"
redshift_india_table = "covid19_india_table"

# Define Redshift JDBC URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

# Write DataFrame to Redshift
total_cases.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_india_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()
print("Covid19 India Data loaded to redshift")

total_cases_country.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_global_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()
print("Covid19 Global Data loaded to redshift")

print("ETL Job Completed.")