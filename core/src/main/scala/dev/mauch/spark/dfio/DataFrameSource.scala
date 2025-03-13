package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame

import java.net.URLDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

trait DataFrameSource {
  def read(): DataFrame
}

class StreamingDataFrameSource(
  spark: SparkSession,
  format: String,
  path: String,
  options: Map[String, String],
  isStream: Boolean
) extends DataFrameSource {
  val schemaOpt: Option[StructType] = options.get("schema").map(s => DataType.fromJson(s).asInstanceOf[StructType])
  override def read(): DataFrame = {
    if (isStream) {
      applyWatermark(
        spark.readStream
          .schema(schemaOpt.getOrElse(throw new IllegalArgumentException("schema is required for streaming")))
          .format(format)
          .options(options)
          .load(path)
      )
    } else {
      spark.read
        .format(format)
        .options(options)
        .load(path)
    }
  }
  def applyWatermark(df: DataFrame): DataFrame = {
    val watermark = options.get("watermark").map(w => w.split(":").map(s => URLDecoder.decode(s, "UTF-8").trim))
    watermark match {
      case Some(Array(column, duration)) => df.withWatermark(column, duration)
      case _ => df
    }
  }
}
