package dev.mauch.spark.dfio

import java.net.URLDecoder
import org.apache.spark.sql.DataFrame

trait TransformerParser extends PartialFunction[java.net.URI, DataFrame => DataFrame] {
  def sparkConfigs: Map[String, String] = Map.empty
  def schemes: Seq[String]
  def isDefinedAt(uri: java.net.URI): Boolean = schemes.contains(uri.getScheme)
}

class IdentityTransformerParser extends TransformerParser {
  override def schemes: Seq[String] = Seq("identity")
  override def apply(uri: java.net.URI): DataFrame => DataFrame = identity[DataFrame]
}

class SqlTransformerParser extends TransformerParser {
  override def schemes: Seq[String] = Seq("sql", "sql-file")
  override def apply(uri: java.net.URI): DataFrame => DataFrame = { df =>
    val sql = uri.getScheme match {
      case "sql" => URLDecoder.decode(uri.getPath.substring(1), "UTF-8")
      case "sql-file" => scala.io.Source.fromFile(uri.getPath.substring(1)).mkString
    }
    df.createOrReplaceTempView("input")
    df.sqlContext.sql(sql)
  }
}
