from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySql to Hive Data Migration") \
    .config("spark.jars", "/Users/syedmanzoor/Downloads/mysql-connector-java-8.0.30.jar,") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# JDBC URL for MySQL
jdbc_url = "jdbc:mysql://localhost:3306/covid19" 

# MySQL connection properties
connection_properties = {
    "user": "root",
    "password": "$yed7007",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load the MySQL table into a PySpark DataFrame
print("Extracting Data from Mysql")
covid_19_data = spark.read.jdbc(url=jdbc_url, table="covid_19_data", properties=connection_properties)
print("extraction completed")

total_records_mysql = covid_19_data.count()
print("Total records in mysql: ", total_records_mysql)

covid_19_data.write.mode("overwrite").saveAsTable("datamigration.covid_19_data")
print('Data successfully loaded to hive table: "datamigration.covid_19_data"')


total_records_hive = spark.sql("SELECT COUNT(*) FROM datamigration.covid_19_data").collect()[0][0]
print("Total records inserted into hive: ", total_records_hive)

def data_migration():
    if total_records_mysql == total_records_hive:
        print(f"Data Migration Successful: MySQL and Hive have the same record count of {total_records_mysql} records. The data integrity is maintained.")
    else:
        print(f"Data Migration Warning: Record mismatch detected! MySQL has {total_records_mysql} records, but Hive contains {total_records_hive} records. Please review the migration process for discrepancies.")

data_migration()