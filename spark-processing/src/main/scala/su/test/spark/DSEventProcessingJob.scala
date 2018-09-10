package su.test.spark

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.DecimalType

abstract class DSEventProcessingJob {
  val ss = SparkSession.builder().appName("PurchaseEvents").getOrCreate()
  import ss.implicits._
  var events: Dataset[Event] = null
  var blocks: Dataset[Block] = null
  var locations: Dataset[Location] = null
  var jdbcURL = ""

  def main(args: Array[String]): Unit = {
    val parsedArgs = CMDArgParser.parse(args)
    events = ss.read.csv(parsedArgs.eventsPath)
      .toDF("productName", "productPrice", "purchaseDate", "productCategory", "clientIP")
      .select($"productName", $"productPrice".cast(DecimalType(10, 2)), $"purchaseDate", $"productCategory", $"clientIP")
      .as[Event](Encoders.product[Event])
    jdbcURL = parsedArgs.jdbcURL
    parsedArgs.blocksPath.foreach(
      path => blocks = ss.read.csv(path)
        .toDF("network", "geonameId", "registeredCountryGeonameID", "representedCountryGeonameID", "isAnon", "isSatellite")
        .as[Block](Encoders.product[Block]))
    parsedArgs.locationsPath.foreach(
      path => locations = ss.read.csv(path)
        .toDF("geonameId", "localeCode", "continentCode", "continentName", "countryISOCode", "countryName", "isInEU")
        .as[Location](Encoders.product[Location]))

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

case class Event(productName: String, productPrice: BigDecimal, purchaseDate: String, productCategory: String, clientIP: String)

case class Block(network: String, geonameId: String)

case class Location(geonameId: String, localeCode: String, continentCode: String, continentName: String, countryISOCode: String, countryName: String)
