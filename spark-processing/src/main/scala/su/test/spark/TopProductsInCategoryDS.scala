package su.test.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}

object TopProductsInCategoryDS extends DSEventProcessingJob {

  import ss.implicits._
  
  override def process(): DataFrame = events.groupBy("productCategory", "productName")
    .count().withColumn("rank", functions.rank().over(Window.partitionBy("productCategory").orderBy($"count".desc)))
    .filter($"rank".leq(10))
    .select($"productCategory".as("product_category"), $"productName".as("product_name"), $"count".as("product_count"))

  override def RDBMSTableName(): String = "top_products_in_categories"
}
