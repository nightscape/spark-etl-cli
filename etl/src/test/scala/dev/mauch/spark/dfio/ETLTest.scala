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
import java.nio.file.{Files, Path, Paths}
import java.net.URLEncoder
import java.time.Instant
object ETLTest extends ZIOSpecDefault {
  val sparkLayer: ZLayer[Scope, Throwable, SparkSession] = ZLayer.scoped(ZIO.acquireRelease {
    ZIO.attempt {
      SparkSession
        .builder()
        .appName("ETLTest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    }.debug
  }(spark => ZIO.succeed(spark.close())))

  case class Person(id: Long, name: String, age: Int)
  val exampleData: List[Person] = List(
    Person(1, "Alice", 30),
    Person(2, "Bob", 25),
    Person(3, "Charlie", 35),
    Person(4, "Dave", 40),
    Person(5, "Eve", 22)
  )
  case class BossRelation(employeeId: Long, bossId: Option[Long], bossSince: Option[Instant] = None)
  val bossRelations: List[BossRelation] = exampleData.zipWithIndex.map { case (person, idx) =>
    BossRelation(
      person.id,
      exampleData.filter(_.id > person.id).sortBy(_.id).headOption.map(_.id),
      Some(Instant.now().minusSeconds(1000).plusSeconds(idx * 10))
    )
  }
  case class Employee(id: Long, name: String, bossId: Option[Long], employeeSince: Option[Instant] = None)
  val employees: List[Employee] = exampleData.zipWithIndex.map { case (person, idx) =>
    Employee(
      person.id,
      person.name,
      bossRelations.find(_.employeeId == person.id).flatMap(_.bossId),
      Some(Instant.now().minusSeconds(100).plusSeconds(idx * 10))
    )
  }
  val tempDirLayer: ZLayer[Scope, Throwable, Path] = ZLayer {
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("dataframe-io-test")))(dir =>
      ZIO.attempt(deleteRecursively(dir)).ignoreLogged
    )
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      Files.list(path).forEach(deleteRecursively)
    }
    Files.delete(path)
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

  def spec = suite("ETL Test")(
    test("should run ETL from Kafka to Delta") {
      for {
        spark <- ZIO.service[SparkSession]
        testDeltaPath <- ZIO.service[Path]
        randomNumber <- Random.nextIntBounded(1000)
        topic <- ZIO.succeed(s"test-topic-$randomNumber")
        kafka <- ZIO.service[KafkaContainer]
        bootstrapServers = kafka.bootstrapServers
        producer <- Producer.make(producerSettings(bootstrapServers))
        _ <- ZIO.foreach(exampleData) { person =>
          val json = s"""{"id": ${person.id}, "name": "${person.name}", "age": ${person.age}}"""
          producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string)
        }
        _ <- ZIO.attempt {
          val args = s"""
            --master local[*]
            --source kafka://${bootstrapServers.replaceFirst("PLAINTEXT://", "")}/$topic?serde=json
            --sink console://foo
            --sink delta://$testDeltaPath
          """.split("\\s+").filter(_.nonEmpty)
          println(args.mkString(" "))
          ETL.main(args)
        }
        result <- ZIO.attempt {
          val deltaDF = spark.read.format("delta").load(testDeltaPath.toString())
          val rows = deltaDF.collect()
          rows
        }

      } yield {
        assert(result.length)(equalTo(exampleData.length)) &&
        assert(result.map(_.getAs[Long]("id")).toSet)(equalTo(exampleData.map(_.id).toSet)) &&
        assert(result.map(_.getAs[String]("name")).toSet)(equalTo(exampleData.map(_.name).toSet))
      }
    } @@ TestAspect.timeout(60.seconds),
    test("should run streaming ETL from Kafka to Delta") {
      val thisExampleData = exampleData.map(person => Person(person.id, person.name, person.age))
      for {
        spark <- ZIO.service[SparkSession]
        testDeltaPath <- ZIO.service[Path]
        randomNumber <- Random.nextIntBounded(1000).map(_ + 1000)
        topic <- ZIO.succeed(s"test-topic-$randomNumber")
        kafka <- ZIO.service[KafkaContainer]
        bootstrapServers = kafka.bootstrapServers
        producer <- Producer.make(producerSettings(bootstrapServers))
        _ <- ZIO.attempt {
          val bossRelationsDF = spark.createDataFrame(bossRelations.take(0))
          bossRelationsDF.write.format("delta").mode("overwrite").save(s"$testDeltaPath/bossRelations")
        }
        sql = """
          SELECT
            e.id,
            e.name,
            br.bossId as bossId
          FROM
            employees e
          JOIN
            bossRelations br
          ON
            e.id = br.employeeId
          AND
            br.bossSince > e.employeeSince
        """.trim
        // Start the streaming ETL job concurrently in a fiber.
        etlFiber <- ZIO
          .attempt {
            val schema = org.apache.spark.sql.Encoders.product[Employee].schema
            val bossRelationsSchema = org.apache.spark.sql.Encoders.product[BossRelation].schema
            val schemaURL = java.net.URLEncoder.encode(schema.json, "UTF-8")
            val bossRelationsSchemaURL = java.net.URLEncoder.encode(bossRelationsSchema.json, "UTF-8")
            val args = s"""
            --master local[*]
            --source employees+kafka-stream://${bootstrapServers.replaceFirst(
                "PLAINTEXT://",
                ""
              )}/$topic?serde=json:$schemaURL&startingOffsets=earliest
            --source bossRelations+delta://${testDeltaPath}/bossRelations
            --transform employees+employeesWithBosses+sql:///${URLEncoder.encode(sql.replaceAll("\\s+", " "), "UTF-8")}
            --sink employeesWithBosses+delta://$testDeltaPath/employeesWithBosses?checkpointLocation=$testDeltaPath/checkpoint
          """.split("\\s+").filter(_.nonEmpty)
            println("Starting ETL.main with: " + args.mkString(" "))
            ETL.main(args)
          }
          .debug
          .fork

        // Allow the ETL job to fully initialize.
        _ <- ZIO.sleep(2.seconds)

        // Produce records and, after each send, check the intermediate Delta table state.
        assertions <- ZStream
          .fromIterable(employees.zipWithIndex)
          .mapZIO { case (employee, idx) =>
            val json =
              s"""{"id": ${employee.id}, "name": "${employee.name}", "employeeSince": "${employee.employeeSince}"}"""
            for {
              _ <- ZIO.attempt {
                val bossRelationsDF = spark.createDataFrame(bossRelations.filter(_.employeeId == employee.id))
                bossRelationsDF.write.format("avro").mode("append").save(s"$testDeltaPath/bossRelations")
              }
              _ <- producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string)
              assertion <- ZIO
                .attempt {
                  import spark.implicits._
                  spark.read.format("delta").load(s"$testDeltaPath/employeesWithBosses").as[Employee].collect().toSeq
                }
                .debug
                .map(rows => assert(rows)(contains(employee)))
                .filterOrFail(_.isSuccess)(new RuntimeException(s"Row $idx: $employee not found"))
                .retry(Schedule.spaced(5000.milliseconds) && Schedule.recurs(2))
            } yield assertion
          }
          .runCollect
        _ <- etlFiber.interrupt
      } yield {
        assertions.reduce(_ && _)
      }
    } @@ TestAspect.timeout(60.seconds) @@ TestAspect.withLiveClock
  )
    .provideSomeLayer[Scope with KafkaContainer with SparkSession](tempDirLayer)
    .provideSomeLayerShared[Scope with KafkaContainer](sparkLayer)
    .provideSomeLayerShared(DockerLayer.kafkaTestContainerLayer)
}
