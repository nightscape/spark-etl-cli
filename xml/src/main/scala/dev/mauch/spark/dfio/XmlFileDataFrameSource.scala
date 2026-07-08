package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import UriHelpers._

case class XmlFileDataFrameSource(spark: SparkSession, path: String, options: Map[String, String] = Map.empty)
    extends DataFrameSource
    with DataFrameSink {

  private val xsdPath: Option[String] = options.get("xsd")
  // Default the element names to spark-xml's historical defaults. Spark 4's native XML *requires*
  // rowTag, so this keeps the scheme usable without one and consistent across Spark versions.
  private val taggedOptions: Map[String, String] = Map("rowTag" -> "ROW", "rootTag" -> "ROWS") ++ (options - "xsd")
  // xsd drives per-row validation on read; every other query param passes straight to the XML datasource
  private val readOptions: Map[String, String] =
    taggedOptions ++ xsdPath.map("rowValidationXSDPath" -> _)

  override def read(): DataFrame = {
    val reader = spark.read.format("xml").options(readOptions)
    // xsd also supplies the schema, giving typed columns instead of inference
    xsdPath.fold(reader)(p => reader.schema(rowSchemaFromXsd(p))).load(path)
  }

  // XSDToSchema returns the row element wrapped as a single struct field (e.g. the rowTag element);
  // the XML datasource expects the schema of one row's *contents*, so unwrap that single struct.
  private def rowSchemaFromXsd(xsd: String): StructType =
    XsdSchema.read(xsd) match {
      case StructType(Array(StructField(_, inner: StructType, _, _))) => inner
      case other => other
    }

  override def write(df: DataFrame): Boolean = {
    df.write
      .mode(SaveMode.Overwrite)
      .format("xml")
      .options(taggedOptions)
      .save(path)
    true
  }
}

class XmlUriParser extends DataFrameUriParser {
  def schemes: Seq[String] = Seq("xml")
  override def apply(uri: java.net.URI): SparkSession => DataFrameSource with DataFrameSink = { spark =>
    XmlFileDataFrameSource(spark, uri.getPath, options = uri.queryParams)
  }
}
