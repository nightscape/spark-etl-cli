package dev.mauch.spark.dfio

import zio._
import zio.test._
import zio.test.Assertion._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

import java.nio.file.{Files, Path}

object XmlEtlTest extends ZIOSpecDefault {
  private val sparkLayer: ZLayer[Scope, Throwable, SparkSession] = ZLayer.scoped(ZIO.acquireRelease {
    ZIO.attempt(SparkSession.builder().appName("XmlEtlTest").master("local[*]").getOrCreate())
  }(spark => ZIO.succeed(spark.close())))

  private val tempDirLayer: ZLayer[Scope, Throwable, Path] = ZLayer {
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("dataframe-io-xml-test")))(_ => ZIO.unit)
  }

  private def source(uri: String, spark: SparkSession): DataFrameSource with DataFrameSink =
    new XmlUriParser().apply(new java.net.URI(uri))(spark)

  def spec = suite("XML ETL Test")(
    test("round-trips a DataFrame through XML preserving values") {
      for {
        spark <- ZIO.service[SparkSession]
        dir <- ZIO.service[Path]
        out = s"$dir/roundtrip"
        result <- ZIO.attempt {
          import spark.implicits._
          val input = Seq((1L, "Alice"), (2L, "Bob"), (3L, "Charlie")).toDF("id", "name")
          source(s"xml://$out", spark).write(input)
          val readBack = source(s"xml://$out", spark).read()
          readBack.orderBy("id").collect().map(r => (r.getAs[Long]("id"), r.getAs[String]("name"))).toSeq
        }
      } yield assert(result)(equalTo(Seq((1L, "Alice"), (2L, "Bob"), (3L, "Charlie"))))
    },
    test("reads externally-tagged XML by rowTag") {
      for {
        spark <- ZIO.service[SparkSession]
        dir <- ZIO.service[Path]
        xml = writeXml(
          dir,
          "books",
          """<catalog>
            |  <book><id>1</id><title>SICP</title></book>
            |  <book><id>2</id><title>TAPL</title></book>
            |</catalog>""".stripMargin
        )
        titles <- ZIO.attempt {
          source(s"xml://$xml?rowTag=book", spark)
            .read()
            .orderBy("id")
            .collect()
            .map(_.getAs[String]("title"))
            .toSeq
        }
      } yield assert(titles)(equalTo(Seq("SICP", "TAPL")))
    },
    test("derives a typed schema from an XSD") {
      for {
        spark <- ZIO.service[SparkSession]
        dir <- ZIO.service[Path]
        xsd = writeXsd(dir)
        xml = writeXml(
          dir,
          "valid",
          """<people>
            |  <person><id>1</id><name>Alice</name><age>30</age></person>
            |  <person><id>2</id><name>Bob</name><age>25</age></person>
            |</people>""".stripMargin
        )
        schema <- ZIO.attempt(source(s"xml://$xml?rowTag=person&xsd=$xsd", spark).read().schema)
      } yield assert(schema.map(_.name).toSet)(equalTo(Set("id", "name", "age"))) &&
        assert(schema("id").dataType)(equalTo(LongType))
    },
    test("drops rows that fail XSD validation") {
      for {
        spark <- ZIO.service[SparkSession]
        dir <- ZIO.service[Path]
        xsd = writeXsd(dir)
        // the second person is missing the required <name> element -> invalid against the XSD
        xml = writeXml(
          dir,
          "invalid",
          """<people>
            |  <person><id>1</id><name>Alice</name><age>30</age></person>
            |  <person><id>2</id><age>25</age></person>
            |  <person><id>3</id><name>Charlie</name><age>35</age></person>
            |</people>""".stripMargin
        )
        ids <- ZIO.attempt {
          source(s"xml://$xml?rowTag=person&xsd=$xsd&mode=DROPMALFORMED", spark)
            .read()
            .collect()
            .map(_.getAs[Long]("id"))
            .toSeq
            .sorted
        }
      } yield assert(ids)(equalTo(Seq(1L, 3L)))
    }
  ).provideSomeLayer[Scope with SparkSession](tempDirLayer)
    .provideSomeLayerShared[Scope](sparkLayer) @@ TestAspect.sequential

  private def writeXsd(dir: Path): String = {
    val xsd =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        |  <xs:element name="person">
        |    <xs:complexType>
        |      <xs:sequence>
        |        <xs:element name="id" type="xs:long"/>
        |        <xs:element name="name" type="xs:string"/>
        |        <xs:element name="age" type="xs:int"/>
        |      </xs:sequence>
        |    </xs:complexType>
        |  </xs:element>
        |</xs:schema>""".stripMargin
    val path = dir.resolve("person.xsd")
    Files.write(path, xsd.getBytes("UTF-8"))
    path.toString
  }

  private def writeXml(dir: Path, name: String, body: String): String = {
    val path = dir.resolve(s"$name.xml")
    Files.write(path, s"""<?xml version="1.0" encoding="UTF-8"?>\n$body""".getBytes("UTF-8"))
    path.toString
  }
}
