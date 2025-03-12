package dev.mauch.spark.dfio
// scalastyle:off
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.images.builder.ImageFromDockerfile
import zio.testcontainers._
import zio.{Scope, Task, ZIO, ZLayer}

import java.time.Duration
import java.util.Properties
import java.io.File
import java.nio.file.{Files, Paths}
import java.io.PrintWriter
import java.io.FileWriter

import com.dimafeng.testcontainers.WaitingForService
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.containers.output.OutputFrame
import com.dimafeng.testcontainers.ServiceLogConsumer
import com.dimafeng.testcontainers.KafkaContainer

object DockerLayer {

  val kafkaTestContainerLayer: ZLayer[Scope, Throwable, KafkaContainer] =
    ZLayer.fromTestContainer(KafkaContainer())
}
