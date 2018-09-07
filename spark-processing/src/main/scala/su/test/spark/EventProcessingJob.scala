package su.test.spark

import java.sql.DriverManager
import java.util.Properties

import com.mysql.jdbc.Driver
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import su.test.spark.PurchaseEventProcessor.sc

abstract class EventProcessingJob {
  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("PurchaseEvents").set("driver","com.mysql.jdbc.Driver"))
  val ss = SparkSession.builder().appName("PurchaseEvents").getOrCreate()
  var events: RDD[Array[String]] = null
  var geoRange: RDD[Array[String]] = null
  var geoCountry: RDD[Array[String]] = null
  var jdbcString = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Not enough arguments. Expected <events-path> <blocks-path> <locations-path> <jdbc-url>")
      sys.exit(-1)
    }

    val eventsFilePath = args(0)
    val geoRangeFilePath = args(1)
    val geoCountryFilePath = args(2)
    jdbcString = args(3)

    events = sc.textFile(eventsFilePath).map(_.split(","))
    geoRange = sc.textFile(geoRangeFilePath).map(_.split(","))
    geoCountry = sc.textFile(geoCountryFilePath).map(_.split(","))


    val result = process()

//    println("RESULT")
//    result.show()

    saveRDDtoRDBMS(result, RDBMSTableName())
  }

  def process(): DataFrame

  def RDBMSTableName(): String

  private def saveRDDtoRDBMS(entities: DataFrame, tableName: String) = {
    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    entities.write.mode(SaveMode.Append).jdbc(jdbcString, tableName, props)
  }
}
