package dev.mauch.spark.dfio

import zio._
import zio.test._
import zio.test.Assertion._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.stream._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.dimafeng.testcontainers.KafkaContainer
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer.Consumer.AutoOffsetStrategy
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import java.io.File
import java.nio.file.{Files, Paths}

object ETLTest extends ZIOSpecDefault {

  case class Person(id: Long, name: String, age: Int)
  val exampleData: List[Person] = List(
    Person(1, "Alice", 30),
    Person(2, "Bob", 25),
    Person(3, "Charlie", 35),
    Person(4, "Dave", 40),
    Person(5, "Eve", 22),
  )

  private val testTopic = "test-topic"
  private val testDeltaPath = {
    val tempDir = Files.createTempDirectory("dataframe-io-test").toFile()
    tempDir.deleteOnExit()
    tempDir.getAbsolutePath()
  }

  private def producerSettings(bootstrapServers: String): ProducerSettings =
    ProducerSettings(List(bootstrapServers))
      .withClientId("test-producer")
      .withProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")

  private def consumerSettings(bootstrapServers: String): ConsumerSettings =
    ConsumerSettings(List(bootstrapServers))
      .withGroupId("test-group")
      .withClientId("test-consumer")
      .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withCloseTimeout(5.seconds)

  private def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("ETLTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  def spec = suite("ETL Test")(test("should run ETL from Kafka to Delta") {
    for {
      kafka <- ZIO.service[KafkaContainer]
      bootstrapServers = kafka.bootstrapServers
      producer <- Producer.make(producerSettings(bootstrapServers))
      _ <- ZIO.foreach(exampleData) { person =>
        val json = s"""{"id": ${person.id}, "name": "${person.name}", "age": ${person.age}}"""
        producer.produce(new ProducerRecord(testTopic, "key1", json), Serde.string, Serde.string)
      }
      _ <- ZIO.attempt {
        val args = s"""
            --master local[*]
            --source kafka://${bootstrapServers.replaceFirst("PLAINTEXT://", "")}/$testTopic?serde=json
            --sink console://foo
            --sink delta://$testDeltaPath
          """.split("\\s+").filter(_.nonEmpty)
        println(args.mkString(" "))
        ETL.main(args)
      }
      result <- ZIO.attempt {
        val spark = createSparkSession()
        val deltaDF = spark.read.format("delta").load(testDeltaPath)
        val rows = deltaDF.collect()
        spark.close()
        rows
      }

    } yield {
      assert(result.length)(equalTo(exampleData.length)) &&
      assert(result.map(_.getAs[Long]("id")).toSet)(equalTo(exampleData.map(_.id).toSet)) &&
      assert(result.map(_.getAs[String]("name")).toSet)(equalTo(exampleData.map(_.name).toSet))
    }
  } @@ TestAspect.timeout(120.seconds)).provideSomeLayerShared(DockerLayer.kafkaTestContainerLayer)
}
