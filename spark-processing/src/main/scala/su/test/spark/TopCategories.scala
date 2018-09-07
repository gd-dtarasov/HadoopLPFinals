package su.test.spark

import java.util

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters.seqAsJavaListConverter

object TopCategories extends EventProcessingJob {
  override def process(): DataFrame = ss.createDataFrame(toRows(calcTopCategoriesRDD()), DBschema)

  override def RDBMSTableName(): String = "top_categories"

  private def toRows(array: Array[(String, Long)]): util.List[Row] = array.map(e => Row(e._1, e._2)).toList.asJava

  private val DBschema = StructType(
    Seq(
      StructField("product_category", StringType, false),
      StructField("category_count", LongType, false)
    )
  )
  
  private def calcTopCategoriesRDD() = {
    events.map(line => (line(3), 1L)).keyBy(_._1).mapValues(_._2).aggregateByKey(0L)(_ + _, _ + _)
      .keyBy(_._2).mapValues(_._1).top(10)(Ordering.by(_._1)).map(_.swap)
  }
}
