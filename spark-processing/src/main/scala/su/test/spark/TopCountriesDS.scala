package su.test.spark

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession, functions}

object TopCountriesDS extends DSEventProcessingJob {

  import ss.implicits._

  val udf = functions.udf((network: String, ip: String) => TopCountries.isInRange(network, ip))

  override def process(): DataFrame = events.as("events").groupBy("clientIP")
    .agg(functions.sum("productPrice").as("totalIp"))
    .join(blocks.as("blocks"), udf($"blocks.network", $"events.clientIP"))
    .join(locations.as("locations"), $"locations.geonameId" === $"blocks.geonameId")
    .groupBy($"countryName".as("country")).agg(functions.sum("totalIp").as("total"))
    .orderBy($"total".desc)
    .limit(10)

  override def RDBMSTableName(): String = "top_countries_by_total"
}
