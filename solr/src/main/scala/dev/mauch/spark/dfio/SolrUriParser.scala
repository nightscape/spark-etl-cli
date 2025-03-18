package dev.mauch.spark.dfio

import org.apache.spark.sql.SparkSession
import UriHelpers._
import java.net.URI

class SolrUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("solr")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    SolrDataFrameSourceSink(
      spark,
      collection = uri.pathParts.head,
      batchSize = uri.queryParams.get("batchSize").map(_.toInt)
    )
  }
}
