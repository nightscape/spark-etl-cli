package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant

case class ParquetDataFrameIO(spark: SparkSession, path: String) extends DataFrameSource with DataFrameSink {
  override def read(): DataFrame = {
    spark.read.parquet(path)
  }

  override def write(df: DataFrame): Boolean = {
    try {
      df.write.parquet(path)
      true
    } catch {
      case e: Exception => false
    }
  }
}

class ParquetUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("parquet")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new ParquetDataFrameIO(spark, uri.getPath)
  }
}
