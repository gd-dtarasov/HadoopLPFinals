package su.test.spark

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters.seqAsJavaListConverter

object TopCountries extends EventProcessingJob {

  override def process(): DataFrame = ss.createDataFrame(toRows(calcTopCountriesByTotalRDD()), DBSchema)


  override def RDBMSTableName(): String = "top_countries_by_total"

  private val DBSchema = StructType(
    Seq(
      StructField("country", StringType, false),
      StructField("total", DataTypes.createDecimalType(10, 2), false)
    )
  )

  private def toRows(array: Array[(String, BigDecimal)]) = array.map(e => Row(e._1, e._2)).toList.asJava

  private def calcTopCountriesByTotalRDD() = {
    events.cartesian(geoRange).filter(pair => isInRange(pair._2(0), pair._1(4))).map(pair => (pair._2(1), pair._1(1)))
      .keyBy(_._1)
      .mapValues(_._2).mapValues(BigDecimal.exact)
      .reduceByKey(_ + _)
      .join(geoCountry.keyBy(_ (0)).mapValues(_ (5)))
      .filter(e => e._2._2 != null && !e._2._2.isEmpty)
      .map(_._2)
      .top(10)(Ordering.by(_._1))
      .map(_.swap)
  }

  private def isInRange(network: String, ip: String) = {
    try {
      new SubnetUtils(network).getInfo.isInRange(ip)
    } catch {
      case e: Exception => println("Error parsing CIDR " + e.getMessage)
        false
    }

  }
}
