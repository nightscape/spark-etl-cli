package dev.mauch.spark.dfio

import org.apache.spark.sql.SparkSession

import java.net.{URI, URLDecoder}
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.DataFrame

import UriHelpers._

class DataFrameUrlParser(_spark: SparkSession) {
  def unapply(s: String): Option[DataFrameSource with DataFrameSink] = {
    val uri = new java.net.URI(s).parseServerAuthority()
    if (DataFrameUrlParser.isDefinedAt(uri)) {
      Some(DataFrameUrlParser(uri)(_spark))
    } else {
      None
    }
  }

}
object DataFrameUrlParser extends DataFrameUriParser {
  private def registry = java.util.ServiceLoader.load[DataFrameUriParser](classOf[DataFrameUriParser])
  private val uriParsers = registry.iterator().asScala.toList

  // TODO: It would be better to only gather the spark configs for the schemes that are actually used.
  override def sparkConfigs: Map[String, String] = uriParsers.flatMap(_.sparkConfigs).toMap
  def schemes: Seq[String] = uriParsers.flatMap(_.schemes)

  def apply(uri: URI): SparkSession => DataFrameSource with DataFrameSink = {
    println(s"""
               |transform
               |scheme:    ${uri.getScheme}
               |user:      ${uri.getUserInfo}
               |authority: ${uri.getAuthority}
               |fragment:  ${uri.getFragment}
               |host:      ${uri.getHost}
               |port:      ${uri.getPort}
               |path:      ${uri.getPath}
               |query:     ${uri.getQuery}
               |""".stripMargin)

    val providers = registry.iterator().asScala.toList
    providers.collectFirst { case p if p.isDefinedAt(uri) => p(uri) }.get
  }
}

object TransformerUrlParser extends TransformerParser {
  private def registry = java.util.ServiceLoader.load[TransformerParser](classOf[TransformerParser])
  private val uriParsers = registry.iterator().asScala.toList

  // TODO: It would be better to only gather the spark configs for the schemes that are actually used.
  override def sparkConfigs: Map[String, String] = uriParsers.flatMap(_.sparkConfigs).toMap
  def schemes: Seq[String] = uriParsers.flatMap(_.schemes)

  def apply(uri: URI): DataFrame => DataFrame = {
    println(s"""
               |scheme:    ${uri.getScheme}
               |user:      ${uri.getUserInfo}
               |authority: ${uri.getAuthority}
               |fragment:  ${uri.getFragment}
               |host:      ${uri.getHost}
               |port:      ${uri.getPort}
               |path:      ${uri.getPath}
               |query:     ${uri.getQuery}
               |""".stripMargin)

    val providers = registry.iterator().asScala.toList
    providers.collectFirst { case p if p.isDefinedAt(uri) => p(uri) }.get
  }
}
