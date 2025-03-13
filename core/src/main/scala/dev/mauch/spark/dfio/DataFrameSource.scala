package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame

import java.net.URLDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

trait DataFrameSource {
  def read(): DataFrame
}

trait DefaultDataFrameSource extends DataFrameSource {
  def spark: SparkSession
  def format: String
  def path: String
  def options: Map[String, String]
  def read(): DataFrame =
    spark.read
      .format(format)
      .options(options)
      .load(path)
}

class StreamingDataFrameSource(
  val spark: SparkSession,
  val format: String,
  val path: String,
  val options: Map[String, String],
  isStream: Boolean
) extends DefaultDataFrameSource {
  val schemaOpt: Option[StructType] = options.get("schema").map(s => DataType.fromJson(s).asInstanceOf[StructType])
  override def read(): DataFrame = {
    if (isStream) {
      val writerWithOptionalSchema = schemaOpt.foldLeft(spark.readStream)(_.schema(_))
      applyWatermark(
        writerWithOptionalSchema
          .format(format)
          .options(options)
          .load(path)
      )
    } else {
      super.read()
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
