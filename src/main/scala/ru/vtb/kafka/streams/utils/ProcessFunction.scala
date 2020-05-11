package ru.vtb.kafka.streams.utils

import java.sql.Connection
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import oracle.jdbc.pool.OracleDataSource
import org.slf4j.LoggerFactory

import scala.collection.breakOut
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class ProcessFunction(configuration: Configuration, customerID: String) {
  private def logger = LoggerFactory.getLogger(this.getClass)

  private val URL = configuration.dbConfig.url
  private val DRIVER = configuration.dbConfig.driver
  private val USER = configuration.dbConfig.user
  private val PASSWORD = configuration.dbConfig.password
  private val TABLES = configuration.dbConfig.tables

  logger.info(s"Setting connection to $URL ...")

  Class.forName(DRIVER)
  private val connection = new OracleDataSource()
  connection.setURL(URL)
  connection.setUser(USER)
  connection.setPassword(PASSWORD)
  val con: Connection = connection.getConnection()
  if (con.isValid(1)) {
    logger.info(s"Successfully Connected to $URL")
  }

  def queryDatabase(table: String): List[Map[String, String]] = {

    var fullTable = new ListBuffer[Map[String, String]]()

    val statement = con.createStatement()
    val start = Instant.now()
    Try(statement.executeQuery(s"select * from $table WHERE $customerID")) match {
      case Success(query) =>
        logger.info(s"Successfully sent query to table '$table'")
        val md = query.getMetaData
        val columnCount = md.getColumnCount
        var columnNames = new ListBuffer[String]()
        for (i <- 1 to columnCount) {
          columnNames += md.getColumnName(i)
        }
        while (query.next()) {
          var data = new ListBuffer[String]()
          for (i <- 1 to columnCount) {
            data += query.getString(i)
          }
          val row: Map[String, String] = (columnNames zip data) (breakOut)
          fullTable += row
        }
      case Failure(exception) => logger.error(s"Error while querying database '$table': ", exception)
    }
    val end = Instant.now()
    val diffInSecs = ChronoUnit.SECONDS.between(start, end)
    logger.info(s"Querying table '$table' took $diffInSecs seconds.")
    fullTable.toList

  }

  def toJson(query: Any): String = query match {
    case m: Map[String, Any] => s"{${m.map(toJson(_)).mkString(",")}}"
    case t: (String, Any) => s""""${t._1}":${toJson(t._2)}"""
    case ss: Seq[Any] => s"""[${ss.map(toJson(_)).mkString(",")}]"""
    case s: String => s""""$s""""
    case null => "null"
    case _ => query.toString
  }

  def run(): String = {
    val allTables: Map[String, List[Map[String, String]]] = TABLES.map(table => table -> queryDatabase(table)).toMap
    con.close()
    if (con.isClosed()) {
      logger.info(s"Successfully closed Connection to $URL")
    }
    toJson(allTables)
  }
}
