package ru.vtb.kafka.streams.utils

import com.google.gson.Gson
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
  private val tables = configuration.dbConfig.tables
  Class.forName(DRIVER)
  private val connection = new OracleDataSource()
  connection.setURL(URL)
  connection.setUser(USER)
  connection.setPassword(PASSWORD)

  def queryDatabase(table: String): List[Map[String, String]] = {
    var fullTable = new ListBuffer[Map[String, String]]()
    Try(connection.getConnection()) match {
      case Success(con) =>
        logger.info("Successfully Connected")
        val statement = con.createStatement()
        val query = statement.executeQuery(s"select * from $table WHERE $customerID")
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
        con.close()
      case Failure(e) => logger.error("Error while connecting to database: ", e)
    }
    fullTable.toList
  }

  def run(): String = {
    val allTables = tables.map(table => table -> queryDatabase(table)).toMap
    val gson = new Gson
    val jsonString = gson.toJson(allTables)
    jsonString
  }
}