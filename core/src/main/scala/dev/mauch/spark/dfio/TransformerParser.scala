package dev.mauch.spark.dfio

import java.net.URLDecoder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

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

class FlattenTransformerParser extends TransformerParser {
  override def schemes: Seq[String] = Seq("flatten")

  private def flattenSchema(schema: StructType, prefix: Vector[String] = Vector.empty): Seq[Column] = {
    schema.fields.flatMap { field =>
      val currentPath = prefix :+ field.name
      val colName = currentPath.mkString("_")
      field.dataType match {
        case st: StructType => flattenSchema(st, currentPath)
        case _ =>
          val colSelector = (prefix.map(p => s"`$p`") :+ s"`${field.name}`").mkString(".")
          Seq(col(colSelector).alias(colName))
      }
    }
  }

  override def apply(uri: java.net.URI): DataFrame => DataFrame = { df =>
    val flattenedCols = flattenSchema(df.schema)
    if (flattenedCols.isEmpty) {
      df
    } else {
      df.select(flattenedCols: _*)
    }
  }
}

class FlattenAndExplodeTransformerParser extends TransformerParser {
  override def schemes: Seq[String] = Seq("flatten-explode")

  private def flattenStructSelectExpr(schema: StructType, prefix: Vector[String]): Seq[String] = {
    schema.fields.flatMap { field =>
      val currentPath = prefix :+ field.name
      val colSelector = currentPath.map(p => s"`$p`").mkString(".")
      val colAlias = currentPath.mkString("_")
      field.dataType match {
        case st: StructType => flattenStructSelectExpr(st, currentPath)
        case _ => Seq(s"$colSelector as `$colAlias`")
      }
    }
  }

  /** Recursively processes a DataFrame by finding the first struct to flatten or array to explode, applying the
    * transformation, and calling itself on the result.
    */
  @scala.annotation.tailrec
  private def processDataFrame(df: DataFrame): DataFrame = {
    // Find the first field that needs transformation (struct or array)
    df.schema.fields.find(field =>
      field.dataType.isInstanceOf[StructType] || field.dataType.isInstanceOf[ArrayType]
    ) match {
      case Some(fieldToTransform) =>
        // Apply the transformation based on the field type
        val transformedDf = fieldToTransform.dataType match {
          case st: StructType =>
            // Flatten the struct
            val fieldName = fieldToTransform.name
            val flattenedExprs = flattenStructSelectExpr(st, Vector(fieldName))
            val existingCols = df.columns.filterNot(_ == fieldName).map(c => s"`$c`")
            df.selectExpr(existingCols ++ flattenedExprs: _*)

          case at: ArrayType =>
            // Explode the array
            val fieldName = fieldToTransform.name
            // Use a temporary name for the exploded column during processing
            val tempExplodedColName = s"__exploded_${fieldName}"
            val originalColsBeforeExplode = df.columns.filterNot(_ == fieldName).map(c => s"`$c`")

            at.elementType match {
              case nestedSt: StructType =>
                // Array of Structs: Explode and flatten in one go
                val flattenedExprsFromExploded = flattenStructSelectExpr(nestedSt, Vector(tempExplodedColName))
                  .map { expr =>
                    // Adjust the alias to remove the temporary name prefix and use the original field name
                    val parts = expr.split(" as ", 2) // Split only on the first " as "
                    val selector = parts(0) // e.g., "`__exploded_fieldName`.`structField`"
                    val aliasWithTempPrefix =
                      parts(1).stripPrefix("`").stripSuffix("`") // e.g., "__exploded_fieldName_structField"
                    // Replace the temp prefix in the alias with the original array field name prefix
                    val finalAlias = aliasWithTempPrefix.replaceFirst(
                      java.util.regex.Pattern.quote(tempExplodedColName + "_"),
                      fieldName + "_"
                    )
                    s"$selector as `$finalAlias`" // e.g., "`__exploded_fieldName`.`structField` as `fieldName_structField`"
                  }

                // Apply explode and then select the original columns + the newly flattened struct fields
                df.withColumn(tempExplodedColName, explode(col(s"`$fieldName`"))) // Explode array into temp col
                  .selectExpr(
                    originalColsBeforeExplode ++ flattenedExprsFromExploded: _*
                  ) // Select original cols + new flattened cols

              case _ =>
                // Array of Primitives/Arrays: Explode and rename back
                df.withColumn(tempExplodedColName, explode(col(s"`$fieldName`"))) // Explode
                  .drop(s"`$fieldName`") // Drop original array column
                  .withColumnRenamed(tempExplodedColName, fieldName) // Rename exploded column back
            }
        }
        // Recursively process the transformed DataFrame
        processDataFrame(transformedDf)

      case None =>
        // Base case: No more fields to transform, return the current DataFrame
        df
    }
  }

  override def apply(uri: java.net.URI): DataFrame => DataFrame = { df =>
    processDataFrame(df)
  }
}
