import sbt._
import Keys._

lazy val sparkVersion = settingKey[String]("Apache Spark version")

lazy val commonSettings = Seq(
  organization := "dev.mauch.spark.dfio",
  scalaVersion := "2.12.20",
  sparkVersion := "3.3.4",
  resolvers ++= Seq(
    "Cloudera" at "https://repository.cloudera.com/artifactory/libs-release-local/",
    "Confluent" at "https://packages.confluent.io/maven/"
  )
)

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

lazy val root = project
  .in(file("."))
  .settings(commonSettings)
  .aggregate(core, serde, /*hive,*/ delta, excel, kafka, solr, `uri-parser`, etl)

// Artifact naming convention
lazy val artifactName = (version: String, scalaV: String, sparkV: String) =>
  s"dataframe-io_${scalaV}_${sparkV}-${version}"

val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion.value,
      "org.apache.spark" %% "spark-sql" % sparkVersion.value
    )
  )

val serde = project
  .in(file("serde"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % "2.3.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.3",
      "za.co.absa" %% "abris" % "6.4.1"
    )
  )

/*
val hive = project
  .in(file("hive"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies += ("com.hortonworks.hive" %% "hive-warehouse-connector-spark3" % "1.0.0.7.2.17.0-334")
      .exclude("net.minidev", "json-smart")
      .exclude("net.shibboleth.tool", "xmlsectool")
  )
 */

val delta = project
  .in(file("delta"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies += "io.delta" %% "delta-core" % "2.3.0")

val avro = project
  .in(file("avro"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion.value)

val excel = project
  .in(file("excel"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies += "dev.mauch" %% "spark-excel" % s"${sparkVersion.value}_0.30.2")

val kafka = project
  .in(file("kafka"))
  .settings(commonSettings)
  .dependsOn(core, serde)
  .settings(libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion.value)

val solr = project
  .in(file("solr"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.solr" % "solr-solrj" % "9.8.1",
      "commons-codec" % "commons-codec" % "1.18.0"
    )
  )

val diff = project
  .in(file("diff"))
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies += "uk.co.gresearch.spark" %% "spark-extension" % s"2.13.0-${sparkVersion.value.split("\\.").take(2).mkString(".")}"
  )

val `uri-parser` = project
  .in(file("uri-parser"))
  .settings(commonSettings)
  .dependsOn(core, serde)
  .settings(
    scalacOptions += "-Xexperimental",
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.13.0"
  )

val etl = project
  .in(file("etl"))
  .settings(commonSettings)
  .dependsOn(
    core,
    serde,
    `uri-parser`,
    kafka % "test->test",
    delta % "test->test",
    avro % "test->test",
    diff % "test->test"
  )
  .enablePlugins(AssemblyPlugin, BuildInfoPlugin)
  .settings(buildInfoSettings)
  .settings(
    libraryDependencies ++= Seq("com.lihaoyi" %% "mainargs" % "0.7.6"),
    assembly / assemblyMergeStrategy := {
      case "META-INF/services/*" => MergeStrategy.concat
      case "application.conf" => MergeStrategy.concat
      case "*.conf" => MergeStrategy.concat
      case ".temp" => MergeStrategy.discard
      case "scala/**" => MergeStrategy.discard
      case "org/apache/spark/**" => MergeStrategy.first // Retain application's Spark classes
    },
    mainClass := Some("dev.mauch.spark.dfio.ETL"),
    Compile / resources += file("../README.md"),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test" % "2.1.16",
      "dev.zio" %% "zio-test-sbt" % "2.1.16",
      "org.testcontainers" % "testcontainers" % "1.20.6",
      "com.dimafeng" %% "testcontainers-scala-core" % "0.43.0",
      "com.dimafeng" %% "testcontainers-scala-kafka" % "0.43.0",
      "com.github.sideeffffect" %% "zio-testcontainers" % "0.6.0",
      "dev.zio" %% "zio-kafka" % "2.6.0",
      "org.apache.kafka" % "kafka-clients" % "3.6.2"
    ).map(_ % Test)
  )

// BuildInfo configuration for ETL module
lazy val buildInfoSettings = Seq(
  buildInfoPackage := "dev.mauch.spark.dfio",
  buildInfoKeys := Seq[BuildInfoKey](
    name,
    version,
    scalaVersion,
    sparkVersion,
    BuildInfoKey.action("formattedShaVersion") { git.formattedShaVersion.value },
    BuildInfoKey.action("readme") { IO.read(file("./README.md")) }
  )
)

// Publishing configuration
publishTo := sonatypePublishToBundle.value

licenses := Seq("MIT License" -> url("http://opensource.org/licenses/MIT"))

pomExtra :=
  <url>https://github.com/nightscape/dataframe-io</url>
  <developers>
    <developer>
      <id>nightscape</id>
      <name>Martin Mauch</name>
      <url>https://github.com/nightscape</url>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:https://github.com/nightscape/dataframe-io.git</connection>
    <developerConnection>scm:git:ssh://github.com:nightscape/dataframe-io.git</developerConnection>
    <url>https://github.com/nightscape/dataframe-io</url>
  </scm>

// Git-based versioning
enablePlugins(GitVersioning)
