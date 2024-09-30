import org.apache.spark.sql.SparkSession

object hiveTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hive Test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("SHOW DATABASES").show()
    // Example of creating a Hive table and loading data
    spark.sql("CREATE DATABASE IF NOT EXISTS testdb")
    spark.sql("USE testdb")
    spark.sql("CREATE TABLE IF NOT EXISTS employee (id INT, name STRING, salary FLOAT)")
    spark.sql("INSERT INTO employee VALUES (1, 'John', 1000.0)")

    // Query the Hive table
    val df = spark.sql("SELECT * FROM employee")
    df.show()
  }
}