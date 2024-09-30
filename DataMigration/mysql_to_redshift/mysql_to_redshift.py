from pyspark.sql import SparkSession

# Initialize a Spark session with the necessary JARs and resource configurations
spark = SparkSession.builder \
    .appName("MySQL to Redshift Migration - Full Data") \
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

# Load only one record from MySQL into a Spark DataFrame
query = "(SELECT * FROM covid_19_data WHERE country_region = 'India') AS temp"
covid19_data = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Save the single record DataFrame to a temporary S3 location in CSV format
covid19_data.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3a://syedmanzoor/staging_data/covid19_India/")
print("Test data stored to S3 bucket")

# Parameters for the Redshift connection
redshift_host = "default-workgroup.050752616685.eu-north-1.redshift-serverless.amazonaws.com"
redshift_db = "dev"
redshift_user = "admin"
redshift_password = "$Yed7007"
redshift_port = 5439

# Target Redshift table
redshift_table = "covid19_India"

# Define Redshift JDBC URL and properties
redshift_url = f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}"
connection_properties = {
    "user": redshift_user,
    "password": redshift_password,
    "driver": "com.amazon.redshift.jdbc.Driver"
}

# Write the single record DataFrame to Redshift
covid19_data.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()
print("Data loaded to redshift")