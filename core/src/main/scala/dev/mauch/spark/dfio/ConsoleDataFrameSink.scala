package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant

case class ConsoleDataFrameSink(spark: SparkSession) extends DataFrameSource with DataFrameSink {
  override def read(): DataFrame =
    spark.emptyDataFrame
  override def write(df: DataFrame): Boolean = {
    if (df.isStreaming) {
      df.writeStream.outputMode("append").format("console").start()
    } else {
      df.show(10000, false)
    }
    true
  }
}

class ConsoleUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("console")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    new ConsoleDataFrameSink(spark)
  }
}
