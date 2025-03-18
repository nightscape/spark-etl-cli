package dev.mauch.spark.dfio

import com.hortonworks.hwc.HiveWarehouseSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.net.URI
import UriHelpers._

case class HiveDataFrameSource(spark: SparkSession, dbName: String, tableName: String, partitionCols: Seq[String])
    extends DataFrameSource
    with DataFrameSink {
  private val hive = HiveWarehouseSession.session(spark).build()
  override def read(): DataFrame = {
    val statement = s"select * from $dbName.$tableName"
    hive.sql(statement)
  }

  override def write(df: DataFrame): Boolean = {
    // Hive throws an exception when trying to remove temp files if the DataFrame is empty
    if (df.cache().count() > 0) {
      hive.setDatabase(dbName)
      val targetSchema = hive.table(tableName).schema
      // In order for this to work, you need to add sth. like the following to your spark-defaults.conf:
      // spark.datasource.hive.warehouse.load.staging.dir=s3a://devha-cdp-stba/data/tmp/
      df.select(targetSchema.names.map(col): _*)
        .write
        .format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR)
        .mode("append")
        .option("table", tableName)
        .save()
      true
    } else {
      false
    }
  }
}

object HiveUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("hive")
  def apply(uri: URI): SparkSession => DataFrameSource with DataFrameSink =
    spark =>
      HiveDataFrameSource(
        spark,
        dbName = uri.pathParts(0),
        tableName = uri.pathParts(1),
        partitionCols = uri.queryParams
          .getOrElse("partitionCols", "")
          .split(",")
          .filter(_.nonEmpty)
      )
}
