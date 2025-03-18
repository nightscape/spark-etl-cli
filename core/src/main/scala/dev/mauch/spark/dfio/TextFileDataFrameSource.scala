package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.Instant

import UriHelpers._

case class TextFileDataFrameSource(spark: SparkSession, path: String, delimiter: String = ",", header: Boolean = true)
    extends DataFrameSource
    with DataFrameSink {

  override def read(): DataFrame = {
    spark.read.option("header", header).option("delimiter", delimiter).csv(path)
  }

  override def write(df: DataFrame): Boolean = {
    df.select(DataFrameUtils.flattenSchema(df.schema): _*)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)
    true
  }
}

class TextFileUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("text")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    val delimiter = uri.pathParts.last.split("\\.").lastOption match {
      case Some("csv") => ","
      case Some("tsv") => "\t"
      case _ => ","
    }
    new TextFileDataFrameSource(
      spark,
      uri.getPath,
      delimiter = delimiter,
      header = uri.queryParams.getOrElse("header", "true").toBoolean
    )
  }
}
