package su.test.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable

object TopProductsInCategory extends RDDEventProcessingJob {
  override def process(): DataFrame = ss.createDataFrame(toRows(calcTopProductsInEveryCategoryRDD()), DBSchema)

  override def RDBMSTableName(): String = "top_products_in_categories"

  private def toRows(rdd: RDD[(String, List[(String, Long)])]) =
    rdd.map(category => category._2.map(product => Row(category._1, product._1, product._2))).flatMap(identity)

  private val DBSchema = StructType(
    Seq(
      StructField("product_category", StringType, false),
      StructField("product_name", StringType, false),
      StructField("product_count", LongType, false)
    )
  )

  private def calcTopProductsInEveryCategoryRDD() = {
    val initialCollection = mutable.Map.empty[String, Long]

    def addToMap(map: mutable.Map[String, Long], element: Array[String]) = {
      val productName = element(0)
      map.put(productName, map.getOrElseUpdate(productName, 0) + 1)
      map
    }

    def getTop(map: mutable.Map[String, Long]) = {
      map.toList.sortWith(_._2 > _._2).take(10)
    }

    def mergePartitions(set1: mutable.Map[String, Long], set2: mutable.Map[String, Long]) = {
      mutable.Map[String, Long]((set1.toList ++ set2.toList).groupBy(_._1).map(st => (st._1, st._2.map(_._2).sum)).toSeq: _*)
    }

    events.keyBy(_(3)).aggregateByKey(initialCollection)(addToMap, mergePartitions).mapValues(getTop)
  }
}
