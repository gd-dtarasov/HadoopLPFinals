package su.test.spark

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

abstract class RDDEventProcessingJob {
  private val sc = SparkContext.getOrCreate(new SparkConf().setAppName("PurchaseEvents").set("driver", "com.mysql.jdbc.Driver"))
  val ss: SparkSession = SparkSession.builder().appName("PurchaseEvents").getOrCreate()
  var events: RDD[Array[String]] = null
  var blocks: RDD[Array[String]] = null
  var locations: RDD[Array[String]] = null
  var jdbcURL = ""

  def main(args: Array[String]): Unit = {
    val parsedArgs = CMDArgParser.parse(args)
    events = sc.textFile(parsedArgs.eventsPath).map(_.split(","))
    jdbcURL = parsedArgs.jdbcURL
    parsedArgs.blocksPath.foreach(path => blocks = sc.textFile(path).map(_.split(",")))
    parsedArgs.locationsPath.foreach(path => locations = sc.textFile(path).map(_.split(",")))
    
    val result = process()

    result.cache()
    result.show()

    saveRDDtoRDBMS(result, RDBMSTableName())
  }

  def process(): DataFrame

  def RDBMSTableName(): String

  private def saveRDDtoRDBMS(entities: DataFrame, tableName: String) = {
    val props = new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    entities.write.mode(SaveMode.Append).jdbc(jdbcURL, tableName, props)
  }
}
