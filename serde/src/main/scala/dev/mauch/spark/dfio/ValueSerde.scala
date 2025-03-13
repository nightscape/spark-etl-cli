package dev.mauch.spark.dfio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.types.DataType
import java.net.URLDecoder

trait ValueSerde {
  def serialize(df: DataFrame): DataFrame
  def deserialize(df: DataFrame): DataFrame
}

object ValueSerde {
  type Constructor = Function2[String, Map[String, String], Option[ValueSerde]]
  val constructors: List[Constructor] = List(NoneSerdeConstructor, JsonSerdeConstructor, AvroSerdeConstructor)
  def apply(serde: String, conf: Map[String, String], topic: String): ValueSerde =
    constructors.flatMap { c => c(serde, conf) }.headOption.getOrElse {
      throw new IllegalArgumentException(s"No constructor found for serde $serde and conf $conf")
    }

}

object NoneSerde extends ValueSerde {
  override def serialize(df: DataFrame): DataFrame = df
  override def deserialize(df: DataFrame): DataFrame = df
}

object NoneSerdeConstructor extends ValueSerde.Constructor {
  override def apply(serde: String, conf: Map[String, String]): Option[ValueSerde] =
    if (serde == "none") Some(NoneSerde) else None
}
