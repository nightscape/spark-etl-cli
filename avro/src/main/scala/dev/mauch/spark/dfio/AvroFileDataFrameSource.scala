package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import java.time.Instant

import UriHelpers._

class AvroFileDataFrameSource(
  spark: SparkSession,
  path: String,
  options: Map[String, String] = Map.empty,
  isStream: Boolean = false
) extends StreamingDataFrameSource(spark, "avro", path, options, isStream)
    with DataFrameSink {

  override def write(df: DataFrame): Boolean = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .options(options)
      .save(path)
    true
  }
}

class AvroUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("avro", "avro-stream")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    val isStream = uri.getScheme == "avro-stream"
    new AvroFileDataFrameSource(spark, uri.getPath, options = uri.queryParams, isStream = isStream)
  }
}
