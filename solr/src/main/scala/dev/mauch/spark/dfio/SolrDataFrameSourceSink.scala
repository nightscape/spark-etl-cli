package dev.mauch.spark.dfio

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpClientUtil, Krb5HttpClientBuilder}
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.SolrInputDocument

import java.util.Optional
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import org.apache.spark.sql._

import java.util

case class SolrDataFrameSourceSink(spark: SparkSession, collection: String, batchSize: Option[Int] = None)
    extends DataFrameSource
    with DataFrameSink
    with Serializable {

  override def read(): DataFrame = ???

  val ExtractFromZookeeperUrl = "(.*?)(/.*)".r("hostPorts", "path")
  override def write(df: DataFrame): Boolean = {
    val conf = df.sparkSession.conf
    val zookeeperUrl = conf.get("spark.solr.zookeeperUrl")
    val ExtractFromZookeeperUrl(hostPorts, zookeeperSolrPath) = zookeeperUrl
    val solrLoginConfigFile = conf.get("spark.solr.loginConfig", "jaas.conf")
    val solrAppName = conf.get("spark.solr.appName", "dataframe-io")

    df.foreachPartition { (rowsIterator: Iterator[Row]) =>
      System.setProperty("java.security.auth.login.config", solrLoginConfigFile)
      System.setProperty("solr.kerberos.jaas.appname", solrAppName)

      val httpClientBuilder = new Krb5HttpClientBuilder().getBuilder()
      HttpClientUtil.setHttpClientBuilder(httpClientBuilder)

      val server =
        new CloudSolrClient.Builder(
          util.Arrays.asList(hostPorts.split(","): _*),
          Optional.ofNullable(Option(zookeeperSolrPath).filter(_.nonEmpty).orNull)
        )
          .build()
      server.setDefaultCollection(collection)
      server.connect()

      def convertTimeStamp(item: Row, fieldName: String): String = {
        if (item.isNullAt(item.fieldIndex(fieldName))) {
          return null
        }
        val datetimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val ldt = java.time.LocalDateTime
          .ofInstant(item.getTimestamp(item.fieldIndex(fieldName)).toInstant, java.time.ZoneId.of("UTC"))
        datetimeFormat.format(ldt)
      }

      val docsIterator = rowsIterator.map { item =>
        val doc = new SolrInputDocument()
        item.schema.foreach { field =>
          val fieldName = field.name
          val fieldValue = item.get(item.fieldIndex(fieldName))
          field.dataType match {
            case TimestampType => doc.addField(fieldName, convertTimeStamp(item, fieldName))
            case _ => doc.addField(fieldName, fieldValue)
          }
        }
        doc
      }

      docsIterator
        .grouped(batchSize.getOrElse(10000))
        .map { docs =>
          val updateRequest = new UpdateRequest()
          updateRequest.add(docs.asJava)
          println(s"Creating update with ${docs.size} docs")
          updateRequest
        }
        .foreach { updateRequest =>
          try {
            println(s"Running update $updateRequest")
            val response = updateRequest.commit(server, collection)
            println(s"Successfully ran update $response")
          } catch {
            case e: java.lang.IllegalArgumentException => println(s"Error: ${e}")
          }
        }
      server.close()
    }
    true
  }
}
