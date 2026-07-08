package dev.mauch.spark.dfio

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.datasources.xml.XSDToSchema
import org.apache.hadoop.fs.Path

// Spark 4.x: XSD -> schema via the native XML datasource in spark-sql.
// read(String) parses the string as XSD *content*, so use the Path overload to read from a file.
object XsdSchema {
  def read(path: String): StructType = XSDToSchema.read(new Path(path))
}
