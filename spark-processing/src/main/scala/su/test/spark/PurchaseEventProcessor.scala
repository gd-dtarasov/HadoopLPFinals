package su.test.spark

import java.util.Properties

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PurchaseEventProcessor {

  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("PurchaseEvents"))
  val ss = SparkSession.builder().appName("PurchaseEvents").getOrCreate()

  def main(args: Array[String]): Unit = {

    //    if (args.length < 3) {
    //      println("Not enough arguments. Expected <events-path> <blocks-path> <locations-path> <jdbc-url>")
    //      sys.exit(-1)
    //    }

    //    val eventsFilePath = args(0)
    //    val geoRangeFilePath = args(1)
    //    val geoCountryFilePath = args(2)
    //    val jdbcString = args(3)
    val eventsFilePath = "input/events.csv"
    val geoRangeFilePath = "input/ranges.csv"
    val geoCountryFilePath = "input/countries.csv"
    val jdbcString = ""
    val events = sc.textFile(eventsFilePath).map(_.split(","))
    val geoRange = sc.textFile(geoRangeFilePath).map(_.split(","))
    val geoCountry = sc.textFile(geoCountryFilePath).map(_.split(","))

    println("COUNT : " + events.count())

    //    def saveRDDtoRDBMS(rdd: RDD[Row], schema: StructType, tableName: String) = {
    //      val props = new Properties();
    //      props.setProperty("driver", "com.mysql.jdbc.Driver")
    //      ss.createDataFrame(rdd, schema).write.mode(SaveMode.Append)
    //        .jdbc(jdbcString, tableName, props)
    //    }
    //
    //    val topCategoriesDFSchema = StructType(
    //      Seq(
    //        StructField("product_category", StringType, false),
    //        StructField("purchase_count", LongType, false)
    //      )
    //    )
    //
    //    val topCategoriesRDD = calcTopCategoriesRDD(events)
    //    val topCategoriesROWRDD = topCategoriesRDD.map(e => Row(e._1, e._2))
    //    saveRDDtoRDBMS(topCategoriesROWRDD, topCategoriesDFSchema, "top_categories")
    //
    //    val topProductsInEveryCategorySchema = StructType(
    //      Seq(
    //        StructField("product_category", StringType, false),
    //        StructField("product_name", StringType, false),
    //        StructField("purchase_count", LongType, false)
    //      )
    //    )
        val topProductsInEveryCategoryROWRDD = calcTopProductsInEveryCategoryRDD(events)
          .map(category => category._2.map(product => Row(category._1, product._1, product._2))).flatMap(identity)
    topProductsInEveryCategoryROWRDD.foreach(println)
    //    saveRDDtoRDBMS(topProductsInEveryCategoryROWRDD, topProductsInEveryCategorySchema, "top_products_in_categories")

//    val topCountriesByTotalRDD = calcTopCountriesByTotalRDD(events, geoRange, geoCountry)
//    topCountriesByTotalRDD.collect().foreach(println)
    //    val topCountriesByTotalROWRDD = topCountriesByTotalRDD.map(e => Row(e._1, e._2))
    //    val topCountriesByTotalSchema = StructType(
    //      Seq(
    //        StructField("country", StringType, false),
    //        StructField("total_spending", DecimalType(2,0), false)
    //      )
    //    )
    //    saveRDDtoRDBMS(topCountriesByTotalROWRDD, topCountriesByTotalSchema, "top_countries_by_total")
  }


  def calcTopCategoriesRDD(input: RDD[Array[String]]) = {
    val resultArray = input.map(line => (line(3), 1L)).keyBy(_._1).mapValues(_._2).aggregateByKey(0L)(_ + _, _ + _)
      .keyBy(_._2).mapValues(_._1).top(10)(Ordering.by(_._1)).map(_.swap)
    sc.parallelize(resultArray)
  }

  def calcTopProductsInEveryCategoryRDD(input: RDD[Array[String]]) = {
    val initialCollection = mutable.HashMap.empty[String, Long]

    def addToMap(map: mutable.HashMap[String, Long], element: Array[String]) = {
      val productName = element(0)
      map.put(productName, map.getOrElseUpdate(productName, 0) + 1)
      map
    }

    def getTop(map: mutable.HashMap[String, Long]) = {
      map.toList.sortWith(_._2 > _._2).take(10)
    }

    def mergePartitions(set1: mutable.HashMap[String, Long], set2: mutable.HashMap[String, Long]) = set1 ++= set2

    input.keyBy(_ (3)).aggregateByKey(initialCollection)(addToMap, mergePartitions).mapValues(getTop)
  }

  def calcTopCountriesByTotalRDD(events: RDD[Array[String]], range: RDD[Array[String]], country: RDD[Array[String]]) = {
    val resultArray = events.cartesian(range).filter(pair => isInRange(pair._2(0), pair._1(4)))
          .map(pair => (pair._2(1), pair._1(1))).keyBy(_._1)
          .mapValues(_._2).mapValues(BigDecimal.exact)
          .reduceByKey(_ + _).join(country.keyBy(_ (0)).mapValues(_ (5))).map(_._2).top(10)(Ordering.by(_._1))
          .map(_.swap)
        sc.parallelize(resultArray)
  }

  def isInRange(network: String, ip: String) = new SubnetUtils(network).getInfo.isInRange(ip)
}
