package ru.vtb.kafka.streams

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import ru.vtb.kafka.streams.utils.{ConfigurationParser, ProcessFunction}
import Serdes._


/**
 * From https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#scala-dsl
 */
object Main extends App {
  val logger = Logger(Main.getClass)

  val configuration = new ConfigurationParser().parseConfig(args(0))


  val props: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, s"${configuration.applicationName}")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"${configuration.kafkaConfig.host}")

    properties.put(ProducerConfig.ACKS_CONFIG, "all")

    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, s"${configuration.kafkaConfig.truststoreLocation}")
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, s"${configuration.kafkaConfig.keystoreLocation}")
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, s"${configuration.kafkaConfig.truststorePassword}")
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, s"${configuration.kafkaConfig.keystorePassword}")
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, s"${configuration.kafkaConfig.keyPassword}")
    properties
  }

  val builder: StreamsBuilder = new StreamsBuilder

  val source: KStream[String, String] = builder.stream[String, String](s"${configuration.kafkaConfig.inputTopic}")

  val process = source
    .map((k, v) => {
      logger.info(s"Received new event ($k->$v)")
      (k, v)
    }
    )
    .mapValues((k, v) => {
      logger.info(s"Processing data from database...")
      val json = new ProcessFunction(configuration, v).run()
      logger.info(s"Sending new data: $json")
      json
    })

  val output: Unit = process.to(s"${configuration.kafkaConfig.outputTopic}")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)

  logger.info("Service Started")
  streams.start()
}