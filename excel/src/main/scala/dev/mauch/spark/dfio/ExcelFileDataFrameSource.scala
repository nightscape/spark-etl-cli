package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._

case class ExcelFileDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty)
    extends DataFrameSource
    with DataFrameSink {

  val finalOptions = Map("inferSchema" -> "true", "header" -> "true") ++ options
  override def read(): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .options(finalOptions)
      .load(path)
  }

  override def write(df: DataFrame): Boolean = {
    df.select(DataFrameUtils.flattenSchema(df.schema): _*)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.crealytics.spark.excel")
      .options(finalOptions)
      .save(path)
    true
  }
}

class ExcelUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("excel")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new ExcelFileDataFrameSource(spark, uri.getPath, options = uri.queryParams)
  }
}
