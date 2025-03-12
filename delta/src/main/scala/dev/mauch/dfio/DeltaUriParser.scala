package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.Row
import java.net.URLDecoder

case class DeltaDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty)
    extends DataFrameSource
    with DataFrameSink {

  override def read(): DataFrame = {
    spark.read
      .format("delta")
      .options(options)
      .load(path)
  }

  def applyTriggerInterval(df: DataStreamWriter[Row]): DataStreamWriter[Row] = {
    options.get("trigger-interval") match {
      case Some(interval) => df.trigger(Trigger.ProcessingTime(URLDecoder.decode(interval, "UTF-8")))
      case None => df
    }
  }
  override def write(df: DataFrame): Boolean = {
    df.write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .options(options)
      .save(path)
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
