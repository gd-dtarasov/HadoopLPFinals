name := "spark-processing"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
//  "org.apache.spark" %% "spark-core" % "2.3.0",
//  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "mysql" % "mysql-connector-java" % "5.1.47",
  "commons-net" % "commons-net" % "3.6"
)