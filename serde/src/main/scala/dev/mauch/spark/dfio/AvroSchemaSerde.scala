package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}
import za.co.absa.abris.avro.functions.{from_avro, to_avro}
import za.co.absa.abris.config.AbrisConfig

import java.nio.file.{Files, Paths}

class AvroSchemaSerde(schema: String, schemaId: Int) extends ValueSerde {
  val config =
    AbrisConfig.toSimpleAvro
      .provideSchema(schema)
      .withSchemaId(schemaId)
  override def serialize(df: DataFrame): DataFrame =
    df.select(to_avro(struct("*"), config).as("value"))
  override def deserialize(df: DataFrame): DataFrame =
    df.select(from_avro(col("value"), schema))
}

object AvroSchemaSerde {
  def unapply(sparkConfig: Map[String, String]): Option[AvroSchemaSerde] = {
    sparkConfig.get("schema.id").map(_.toInt).flatMap { schemaId =>
      sparkConfig
        .get("schema.file.path")
        .map { schemaFilePath =>
          val schema = new String(Files.readAllBytes(Paths.get(schemaFilePath)))
          new AvroSchemaSerde(schema, schemaId)
        }
        .orElse {
          sparkConfig.get("schema.resource.path").map { schemaResourcePath =>
            implicit val codec = scala.io.Codec.UTF8
            val schema = scala.io.Source
              .fromInputStream(getClass.getResourceAsStream("/" + schemaResourcePath))
              .mkString
            new AvroSchemaSerde(schema, schemaId)
          }
        }
    }
  }
}

object AvroSerdeConstructor extends ValueSerde.Constructor {
  override def apply(serde: String, conf: Map[String, String]): Option[ValueSerde] = {
    val schema = conf.get("schema")
    if (serde == "avro" && schema.isDefined) {
      conf match {
        case AvroSchemaRegistrySerde(serde) => Some(serde)
        case AvroSchemaSerde(serde) => Some(serde)
        case _ => None
      }
    } else None
  }
}
