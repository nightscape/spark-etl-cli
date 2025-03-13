package dev.mauch.spark.dfio

import mainargs._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.time.Instant
import scala.util.{Failure, Success, Try}
import UriHelpers._

case class Transformation(inputName: String, outputName: String, transformation: DataFrame => DataFrame) {
  def run(spark: SparkSession): DataFrame = {
    val input = spark.table(inputName)
    val output = transformation(input)
    output.createOrReplaceTempView(outputName)
    output
  }
}

case class Source(sourceName: String, reader: SparkSession => DataFrameSource) {
  def run(spark: SparkSession): DataFrame = {
    val df = reader(spark).read()
    df.createOrReplaceTempView(sourceName)
    df
  }
}

case class Sink(sinkName: String, writer: SparkSession => DataFrameSink) {
  def run(spark: SparkSession): Boolean = {
    val df = spark.table(sinkName)
    writer(spark).write(df)
  }
}

@main
case class ETLOptions(
  @arg(doc = "URIs of the sources") source: List[Source],
  @arg(doc = "URIs of the sinks") sink: List[Sink],
  @arg(doc = "Transformations to apply") transform: List[Transformation] = List(),
  @arg(doc = "The Spark app name") appName: String = "ETL",
  @arg(doc = "The Spark master") master: Option[String] = None
)

object ETLOptions {
  class SourceOrSinkReader[T, U](val shortName: String, wrap: (String, SparkSession => T) => U)
      extends TokensReader.Simple[U] {

    def read(strs: Seq[String]) = Try(URI.create(strs.last)) match {
      case Success(uri) =>
        val (scheme, name) = uri.schemeAndName match {
          case (scheme, nameOption) =>
            (scheme, nameOption.map(_.replaceAll("-", "_")).getOrElse(shortName))
        }
        val newUri =
          new URI(scheme, uri.getUserInfo, uri.getHost, uri.getPort, uri.getPath, uri.getQuery, uri.getFragment)
        DataFrameUrlParser.lift
          .apply(newUri)
          .map(f => wrap(name, f.andThen(_.asInstanceOf[T])))
          .toRight(
            s"URI scheme '${uri.getScheme}' not in supported schemes: ${DataFrameUrlParser.schemes.map(t => s"'$t'").mkString(", ")}"
          )
      case Failure(exception) =>
        Left(exception.getLocalizedMessage)
    }
  }

  implicit object SourceReader extends SourceOrSinkReader[DataFrameSource, Source]("source", Source.apply)
  implicit object SinkReader extends SourceOrSinkReader[DataFrameSink, Sink]("sink", Sink.apply)
  implicit object InstantReader extends TokensReader.Simple[Instant] {
    def shortName = "instant"
    def read(strs: Seq[String]): Either[String, Instant] = Try(strs.last.toLong) match {
      case Success(millis) => Right(Instant.ofEpochMilli(millis))
      case Failure(exception) =>
        Left(s"Wrong Millis: ${exception.toString}")
    }
  }
  implicit object TransformerReader extends TokensReader.Simple[Transformation] {
    def shortName = "transform"
    def read(strs: Seq[String]): Either[String, Transformation] = Try(URI.create(strs.last)) match {
      case Success(uri) =>
        val (scheme, source, sink) = uri.schemeSourceSink match {
          case (scheme, sourceOption, sinkOption) =>
            (
              scheme,
              sourceOption.map(_.replaceAll("-", "_")).getOrElse("source"),
              sinkOption.map(_.replaceAll("-", "_")).getOrElse("sink")
            )
        }
        val newUri =
          new URI(scheme, uri.getUserInfo, uri.getHost, uri.getPort, uri.getPath, uri.getQuery, uri.getFragment)
        TransformerParser
          .unapply(newUri)
          .map(Transformation(source, sink, _))
          .toRight(s"URI scheme ${uri.getScheme} in URI '$uri'")
      case Failure(exception) =>
        Left(s"Invalid URI ${strs}:\n${exception.getLocalizedMessage}")
    }
  }
}

import ETLOptions._

object ETL {
  def main(args: Array[String]): Unit = {
    val doc =
      s"ETL ${BuildInfo.version}, compiled with Scala ${BuildInfo.scalaVersion} for Spark ${BuildInfo.sparkVersion}"
    val readme = BuildInfo.readme
    val options = ParserForClass[ETLOptions].constructOrThrow(args, customDoc = s"$doc\n")
    val sparkBuilder = SparkSession.builder().appName(options.appName)
    val sparkConfigs = DataFrameUrlParser.sparkConfigs
    val configuredBuilder = sparkConfigs.foldLeft(sparkBuilder) { case (builder, (key, value)) =>
      builder.config(key, value)
    }
    val spark =
      options.master.fold(configuredBuilder)(configuredBuilder.master).getOrCreate()
    println(s"Spark session instantiated, version: ${spark.version}")
    options.source.map { source => source.run(spark) }
    val transforms =
      if (options.transform.isEmpty)
        List(Transformation("source", "sink", identity))
      else options.transform
    transforms.foreach(transform => transform.run(spark))
    val successfullyRunSinks = options.sink.takeWhile { sink => sink.run(spark) }
    val success = successfullyRunSinks.length == options.sink.length
    if (success) {
      println("Write successful")
    } else {
      println("Writing to sink failed")
    }
  }
}
