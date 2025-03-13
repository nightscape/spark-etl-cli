package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import java.time.Instant

case class KafkaDataFrameSource(
  spark: SparkSession,
  broker: String,
  topic: String,
  serde: String = "json",
  isStream: Boolean = false,
  additionalOptions: Map[String, String] = Map.empty
) extends DataFrameSource
    with DataFrameSink {
  private val serdeInstance = ValueSerde(
    serde,
    spark.conf.getAll.collect {
      case (key, value) if key.startsWith("spark.kafka.schema") => key.replace("spark.kafka.", "") -> value
    } ++ Map("schema.topic.name" -> topic),
    topic
  )

  override def read(): DataFrame = {
    val df = if (isStream) {
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .options(additionalOptions)
        .load()
    } else {
      spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .options(additionalOptions)
        .load()
    }
    serdeInstance.deserialize(df)
  }

  override def write(df: DataFrame): Boolean = {
    serdeInstance
      .serialize(df)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("topic", topic)
      .options(additionalOptions)
      .save()
    true
  }
}
