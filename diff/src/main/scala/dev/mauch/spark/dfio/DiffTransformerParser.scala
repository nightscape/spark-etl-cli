package dev.mauch.spark.dfio

import java.net.{URI, URLDecoder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import uk.co.gresearch.spark.diff._
import UriHelpers._

class DiffTransformerParser extends TransformerParser {
  override def schemes: Seq[String] = Seq("diff")

  override def apply(uri: URI): DataFrame => DataFrame = { left =>
    // Parse query parameters from the URI
    val params = uri.queryParams

    // Parse id columns if provided; expected as comma separated values
    val idColumns: Seq[String] = params
      .get("id")
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)

    // Parse ignore columns if provided; expected as comma separated values
    val ignoreColumns: Seq[String] = params
      .get("ignore")
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)

    // Extract the right dataframe source path from the URI path, removing any leading slash
    val path = if (uri.getPath.startsWith("/")) uri.getPath.substring(1) else uri.getPath
    if (path.isEmpty) {
      throw new IllegalArgumentException("DiffTransformerParser: Right dataframe source path is empty")
    }

    // Load the right dataframe with the specified format
    val rightDF = left.sparkSession.table(path)

    // Apply the diff transformation with optional id and ignore columns
    val result = if (idColumns.nonEmpty && ignoreColumns.nonEmpty) {
      left.diff(rightDF, idColumns, ignoreColumns)
    } else if (idColumns.nonEmpty) {
      left.diff(rightDF, idColumns: _*)
    } else {
      left.diff(rightDF)
    }

    // Process handleDifferences parameter: 'filter' to filter differences, or 'fail' to throw an exception if differences exist
    val handleDifferences = params.get("handleDifferences").map(_.toLowerCase)
    handleDifferences match {
      case Some("filter") =>
        result.filter(col("diff") =!= lit("N"))
      case Some("fail") =>
        val differences = result.filter(col("diff") =!= lit("N"))
        val diffRows = differences.collect()
        if (diffRows.nonEmpty) {
          throw new RuntimeException("Differences found: " + diffRows.mkString(", "))
        } else {
          result
        }
      case _ => result
    }
  }
}
