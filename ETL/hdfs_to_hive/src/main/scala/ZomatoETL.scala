import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ZomatoETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zomato ETL")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // Reading raw data from hdfs
    val zomato_restaurants_india = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://localhost:9000/raw_data/zomato/Zomato_restaurants_in_india.csv")

    // Insert raw data into hive
    zomato_restaurants_india.write
      .mode("overwrite")
      .insertInto("Zomato.zomato_raw_table" )

    println("Successfully Inserted the raw_data in hive")

    // Saving the raw_ data in hdfs
    zomato_restaurants_india.write
      .mode("overwrite")
      .option("header", "true")
      .csv("hdfs://localhost:9000/user/hive/warehouse/raw_data/zomato_raw_table")

    println("raw_data stored in hdfs://localhost:9000/user/hive/warehouse/raw_data/zomato_raw_table")

    // select columns to be transformed
    val zomato_restaurants_req_columns = zomato_restaurants_india.select("res_id", "name", "establishment", "address", "city", "locality", "cuisines", "timings", "rating_text")

    // renaming the columns
    val zomato_restaurants_renamed = zomato_restaurants_req_columns
      .withColumnRenamed("res_id", "Restaurant_ID")
      .withColumnRenamed("name", "Name")
      .withColumnRenamed("establishment", "Establishment")
      .withColumnRenamed("address", "Address")
      .withColumnRenamed("city", "City")
      .withColumnRenamed("locality", "Locality")
      .withColumnRenamed("cuisines", "Cuisines")
      .withColumnRenamed("timings", "Timing")
      .withColumnRenamed("rating_text", "Ratings")

    println("necessary columns renamed")

    // Mapping of cities to states
    println("mapping cities to state")
    val zomato_restaurants = zomato_restaurants_renamed.withColumn("State",
      when(col("City").isin("Chennai", "Coimbatore", "Madurai", "Trichy", "Salem", "Vellore", "Ooty"), "Tamil Nadu")
        .when(col("City").isin("Bangalore", "Mysore", "Mangalore", "Udupi", "Manipal"), "Karnataka")
        .when(col("City").isin("Hyderabad", "Secunderabad"), "Telangana")
        .when(col("City").isin("Guntur", "Vijayawada", "Tirupati", "Vizag"), "Andhra Pradesh")
        .when(col("City").isin("Mumbai", "Pune", "Nagpur", "Nashik", "Kolhapur", "Aurangabad", "Amravati", "Nasik", "Navi Mumbai", "Thane"), "Maharashtra")
        .when(col("City").isin("New Delhi", "Delhi"), "Delhi")
        .when(col("City").isin("Gurgaon", "Faridabad"), "Haryana")
        .when(col("City").isin("Noida", "Ghaziabad", "Greater Noida", "Allahabad", "Kanpur", "Lucknow", "Varanasi", "Gorakhpur"), "Uttar Pradesh")
        .when(col("City").isin("Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer", "Pushkar", "Neemrana"), "Rajasthan")
        .when(col("City").isin("Kolkata", "Howrah", "Siliguri", "Kharagpur"), "West Bengal")
        .when(col("City").isin("Chandigarh", "Mohali", "Panchkula"), "Chandigarh")
        .when(col("City").isin("Patna", "Gaya"), "Bihar")
        .when(col("City").isin("Goa", "Panaji"), "Goa")
        .when(col("City").isin("Bhopal", "Indore", "Gwalior", "Jabalpur"), "Madhya Pradesh")
        .when(col("City").isin("Bhubaneswar", "Cuttack", "Bhubaneshwar"), "Odisha")
        .when(col("City").isin("Shimla", "Manali", "Dharamshala"), "Himachal Pradesh")
        .when(col("City").isin("Gangtok"), "Sikkim")
        .when(col("City").isin("Jamshedpur", "Ranchi"), "Jharkhand")
        .when(col("City").isin("Jammu", "Srinagar"), "Jammu and Kashmir")
        .when(col("City").isin("Amritsar", "Jalandhar", "Ludhiana", "Patiala", "Nayagaon", "Zirakpur"), "Punjab")
        .when(col("City").isin("Guwahati"), "Assam")
        .when(col("City").isin("Puducherry"), "Puducherry")
        .when(col("City").isin("Trivandrum", "Thrissur", "Palakkad", "Kochi", "Alappuzha"), "Kerala")
        .when(col("City").isin("Meerut", "Haridwar", "Rishikesh", "Dehradun", "Mussoorie", "Nainital"), "Uttarakhand")
        .when(col("City").isin("Surat", "Ahmedabad", "Rajkot", "Gandhinagar", "Vadodara", "Junagadh", "Jamnagar"), "Gujarat")
        .when(col("City").isin("Agra", "Mathura", "Jhansi"), "Uttar Pradesh")
        .when(col("City").isin("Darjeeling"), "West Bengal")
        .when(col("City").isin("Raipur"), "Chhattisgarh")
        .when(col("City").isin("Siliguri"), "West Bengal")
        .otherwise("Unknown"))
    println("mapping cities to state completed")
    zomato_restaurants.show(10)

    // Saving the transformed_data in hdfs
    zomato_restaurants.write
      .mode("overwrite")
      .option("header", "true")
      .csv("hdfs://localhost:9000/user/hive/warehouse/transformed_data/zomato_transformed_table")

    println("transformed_data stored in hdfs://localhost:9000/user/hive/warehouse/transformed_data/zomato_transformed_table")

    // Loading transformed data into hive
    zomato_restaurants.write
      .mode("overwrite")
      .insertInto("Zomato.zomato_transformed_table" )

    println("Loaded transformed data into hive successfully")


  }
}
