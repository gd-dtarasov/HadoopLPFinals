package su.test.spark

import org.apache.spark.sql._

object TopCategoriesDS extends DSEventProcessingJob {

  import ss.implicits._

  override def process(): DataFrame =
    events.groupByKey(_.productCategory).count()
      .select($"value".as("product_category"), $"count(1)".as("category_count"))
      .orderBy($"category_count".desc)
      .limit(10)

  override def RDBMSTableName(): String = "top_categories"
}
