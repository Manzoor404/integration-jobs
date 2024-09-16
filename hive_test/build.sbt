ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.11" // or adjust to 2.12.x if necessary

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-hive" % "3.3.1", // For Hive integration with Spark
  "mysql" % "mysql-connector-java" % "8.0.26"
)

lazy val root = (project in file("."))
  .settings(
    name := "hive_test"
  )