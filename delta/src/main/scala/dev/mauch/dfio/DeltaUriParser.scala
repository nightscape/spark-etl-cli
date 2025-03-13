package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.Row
import java.net.URLDecoder

class DeltaDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty, isStream: Boolean)
    extends StreamingDataFrameSource(spark, "delta", path, options, isStream = isStream)
    with DataFrameSink {

  def applyTriggerInterval(df: DataStreamWriter[Row]): DataStreamWriter[Row] = {
    options.get("trigger-interval") match {
      case Some(interval) => df.trigger(Trigger.ProcessingTime(URLDecoder.decode(interval, "UTF-8")))
      case None => df
    }
  }
  override def write(df: DataFrame): Boolean = {
    if (df.isStreaming) {
      try {
        // Create the delta table if it doesn't exist
        df.limit(0).write.format("delta").options(options).save(path)
      } catch {
        case e: Exception => ()
      }
      applyTriggerInterval(df.writeStream)
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
  def schemes: Seq[String] = Seq("delta", "delta-stream")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    val isStream = uri.getScheme.endsWith("-stream")
    new DeltaDataFrameSource(spark, uri.getPath, options = uri.queryParams, isStream = isStream)
  }
}
