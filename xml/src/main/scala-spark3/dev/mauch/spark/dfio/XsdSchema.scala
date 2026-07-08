package dev.mauch.spark.dfio

import org.apache.spark.sql.types.StructType
import com.databricks.spark.xml.util.XSDToSchema

import java.nio.file.Paths

// Spark 3.x: XSD -> schema via the databricks spark-xml library.
object XsdSchema {
  def read(path: String): StructType = XSDToSchema.read(Paths.get(path))
}
