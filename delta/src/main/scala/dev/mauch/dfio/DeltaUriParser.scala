package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._

case class DeltaDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty)
    extends DataFrameSource
    with DataFrameSink {

  override def read(): DataFrame = {
    spark.read
      .format("delta")
      .options(options)
      .load(path)
  }

  override def write(df: DataFrame): Boolean = {
    if (df.isStreaming) {
      try {
        // Create the delta table if it doesn't exist
        df.limit(0).write.format("delta").options(options).save(path)
      } catch {
        case e: Exception => ()
      }
      df.writeStream
        .format("delta")
        .options(options)
        .start(path)
    } else {
      df.write
        .mode(SaveMode.Overwrite)
        .format("delta")
        .options(options)
        .save(path)
    }
    true
  }
}

class DeltaUriParser extends DataFrameUriParser {
  override def sparkConfigs: Map[String, String] = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )
  def schemes: Seq[String] = Seq("delta")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new DeltaDataFrameSource(spark, uri.getPath, options = uri.queryParams)
  }
}
