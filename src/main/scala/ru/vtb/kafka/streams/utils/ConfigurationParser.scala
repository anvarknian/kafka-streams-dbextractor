package ru.vtb.kafka.streams.utils

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import cats.syntax.either._
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml.parser
import org.slf4j.{Logger, LoggerFactory}

case class DbConfig(url: String,
                    driver: String,
                    user: String,
                    password: String,
                    tables: List[String])

case class kafkaConsumer(host: String,
                         inputTopic: String,
                         outputTopic: String,
                         groupid: String,
                         truststoreLocation: String,
                         truststorePassword: String,
                         keystoreLocation: String,
                         keystorePassword: String,
                         keyPassword: String)

case class Configuration(applicationName: String,
                         dbConfig: DbConfig,
                         kafkaConfig: kafkaConsumer)

class ConfigurationParser {

  private def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def parseConfig(path: String): Configuration = {
    logger.info("Parsing configuration file...")
    val json: Either[ParsingFailure, Json] =
      parser.parse(
        new InputStreamReader(
          new FileInputStream(path), StandardCharsets.UTF_8)
      )

    val values = json
      .leftMap(err => err: Error)
      .flatMap(_.as[Configuration])
      .valueOr(throw _)
    values
  }
}