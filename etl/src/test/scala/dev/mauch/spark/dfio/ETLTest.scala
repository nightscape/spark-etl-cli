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
    Person(5, "Eve", 22),
  )
  case class BossRelation(employeeId: Long, bossId: Option[Long], bossSince: Option[Instant] = None)
  val now = Instant.now()
  val bossRelations: List[BossRelation] = exampleData.zipWithIndex.map { case (person, idx) =>
    BossRelation(
      person.id,
      exampleData.filter(_.id > person.id).sortBy(_.id).headOption.map(_.id),
      Some(now.minusSeconds(100).plusSeconds(idx * 10))
    )
  }
  case class Employee(id: Long, name: String, bossId: Option[Long], employeeSince: Option[Instant] = None)
  val employees: List[Employee] = exampleData.zipWithIndex.map { case (person, idx) =>
    Employee(
      person.id,
      person.name,
      bossRelations.find(_.employeeId == person.id).flatMap(_.bossId),
      Some(now.minusSeconds(1000).plusSeconds(idx * 10))
    )
  }
  println(s"employees: ${employees.mkString("\n")}")
  println(s"bossRelations: ${bossRelations.mkString("\n")}")
  val tempDirLayer: ZLayer[Scope, Throwable, Path] = ZLayer {
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("dataframe-io-test")))(dir =>
      //ZIO.attempt(deleteRecursively(dir)).ignoreLogged
      ZIO.unit
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
            --source expected+values:///?header=id:long,name:string,age:long&values=${exampleData
              .map(person => s"${person.id},${person.name},${person.age}")
              .mkString(";")}
            --transform source+diffResult+diff:///expected?id=id&handleDifferences=filter
            --sink diffResult+console://foo
            --sink diffResult+delta://$testDeltaPath
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
        assert(result)(equalTo(Array.empty[Row]))
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
          bossRelationsDF.write.format("avro").mode("overwrite").save(s"$testDeltaPath/bossRelations")
        }
        sql = """
          SELECT
            e.id,
            e.name,
            br.bossId as bossId,
            e.employeeSince
          FROM
            employees e
          JOIN
            bossRelations br
          ON
            e.id = br.employeeId
          AND
            br.bossSince BETWEEN e.employeeSince AND e.employeeSince %2B INTERVAL 1000 seconds
        """.trim
        _ <- ZIO.attempt {
          val employeesDF = spark.createDataFrame(employees.take(0))
          employeesDF.write.format("delta").mode("overwrite").save(s"$testDeltaPath/employeesWithBosses")
        }
        // Start the streaming ETL job concurrently in a fiber.
        etlFiber <- ZIO
          .attempt {
            val schema = org.apache.spark.sql.Encoders.product[Employee].schema
            val schemaURL = java.net.URLEncoder.encode(schema.json, "UTF-8")
            val bossRelationsSchema = org.apache.spark.sql.Encoders[BossRelation].schema
            val $bossRelationsSchemaURL = java.net.URLEncoder.encode(bossRelationsSchema.json, "UTF-8")
            val args = s"""
            --master local[*]
            --source employees+kafka-stream://${bootstrapServers.replaceFirst(
                "PLAINTEXT://",
                ""
              )}/$topic?serde=json:$schemaURL&startingOffsets=earliest&watermark=employeeSince:1000+seconds
            --source bossRelations+avro-stream://${testDeltaPath}/bossRelations?schema=$bossRelationsSchemaURL&watermark=bossSince:1000+seconds
            --transform employees+employeesWithBosses+sql:///${URLEncoder.encode(sql.replaceAll("\\s+", " "), "UTF-8")}
            --sink employeesWithBosses+delta://$testDeltaPath/employeesWithBosses?checkpointLocation=$testDeltaPath/checkpoint&trigger-interval=100+milliseconds
          """.split("\\s+").filter(_.nonEmpty)
            println("Starting ETL.main with: " + args.mkString(" "))
            ETL.main(args)
          }
          .debug
          .fork

        // Allow the ETL job to fully initialize.
        _ <- ZIO.sleep(2.seconds)

        // Split employees into batches
        batch1 = employees.take(2)
        batch2 = employees.drop(2)

        // Send and check first batch
        _ <- ZStream.fromIterable(batch1)
          .mapZIO { employee =>
            for {
              _ <- ZIO.attempt {
                val bossRelationsDF = spark.createDataFrame(bossRelations.filter(_.employeeId == employee.id))
                bossRelationsDF.write.format("avro").mode("append").save(s"$testDeltaPath/bossRelations")
              }
              json = s"""{"id": ${employee.id}, "name": "${employee.name}"${employee.employeeSince.map(since => s", \"employeeSince\": \"${since}\"").getOrElse("")}}"""
              _ <- producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string)
            } yield ()
          }
          .runDrain

        // Check first batch arrived
        _ <- ZIO
          .attempt(spark.read.format("delta").load(s"$testDeltaPath/employeesWithBosses").count())
          .debug
          .filterOrFail(count => count >= batch1.size)(new RuntimeException("First batch not processed"))
          .retry(Schedule.spaced(500.millis) && Schedule.recurs(20))

        // Send remaining data
        _ <- ZStream.fromIterable(batch2)
          .mapZIO { employee =>
            for {
              _ <- ZIO.attempt {
                val bossRelationsDF = spark.createDataFrame(bossRelations.filter(_.employeeId == employee.id))
                bossRelationsDF.write.format("avro").mode("append").save(s"$testDeltaPath/bossRelations")
              }
              json = s"""{"id": ${employee.id}, "name": "${employee.name}"${employee.employeeSince.map(since => s", \"employeeSince\": \"${since}\"").getOrElse("")}}"""
              _ <- producer.produce(new ProducerRecord(topic, "key1", json), Serde.string, Serde.string)
            } yield ()
          }
          .runDrain

        // Final assertion
        assertion <- ZIO
          .attempt {
            import spark.implicits._
            spark.read.format("delta").load(s"$testDeltaPath/employeesWithBosses").as[Employee].collect().toSeq
          }
          .map(rows => assert(rows)(hasSameElements(employees)))
          .filterOrFail(_.isSuccess)(new RuntimeException("Not all employees found"))
          .retry(Schedule.spaced(1.second) && Schedule.recurs(60))

        // Verify streaming execution
        _ <- ZIO.attempt {
          val df = spark.read.format("delta").load(s"$testDeltaPath/employeesWithBosses")
          val isStreaming = df.queryExecution.logical.isStreaming
          assert(isStreaming)(isTrue)
        }

        // Verify time-based join behavior
        _ <- ZIO.attempt {
          val result = spark.sql(s"""
            SELECT COUNT(*)
            FROM delta.`$testDeltaPath/employeesWithBosses`
            WHERE bossId IS NULL
          """).head().getLong(0)
          assert(result)(equalTo(1L)) // Expect 1 employee without a boss (the last one)
        }

        _ <- ZIO.attempt {
          spark.sql(s"DESCRIBE HISTORY delta.`$testDeltaPath/employeesWithBosses`").show(false)
        }.debug("Delta Table History")

        _ <- etlFiber.interrupt
      } yield {
        assertion
      }
    } @@ TestAspect.timeout(60.seconds) @@ TestAspect.withLiveClock
  )
    .provideSomeLayer[Scope with KafkaContainer with SparkSession](tempDirLayer)
    .provideSomeLayerShared[Scope with KafkaContainer](sparkLayer)
    .provideSomeLayerShared(DockerLayer.kafkaTestContainerLayer)
}
