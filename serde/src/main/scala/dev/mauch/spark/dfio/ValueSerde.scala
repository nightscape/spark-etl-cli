package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}

trait ValueSerde {
  def serialize(df: DataFrame): DataFrame
  def deserialize(df: DataFrame): DataFrame
}

object ValueSerde {
  def apply(serde: String, conf: Map[String, String], topic: String): ValueSerde =
    serde match {
      case "json" => new JsonSerde(None)
      case "avro" =>
        conf match {
          case AvroSchemaRegistrySerde(serde) => serde
          case AvroSchemaSerde(serde) => serde
        }
      case "none" => NoneSerde
    }

}

object NoneSerde extends ValueSerde {
  override def serialize(df: DataFrame): DataFrame = df
  override def deserialize(df: DataFrame): DataFrame = df
}
