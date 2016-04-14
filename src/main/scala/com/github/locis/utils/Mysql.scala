package com.github.locis.utils

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.ResultSet
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.typesafe.config.ConfigFactory

class Mysql {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val mysqlConnection: com.github.mauricio.async.db.mysql.MySQLConnection = {
    val configuration: Configuration = {
      val username: String = ConfigFactory.load.getString("mysql.username")
      val host: String = ConfigFactory.load.getString("mysql.host")
      val port: Int = ConfigFactory.load.getInt("mysql.port")
      val password: String = ConfigFactory.load.getString("mysql.password")
      val database: String = ConfigFactory.load.getString("mysql.database")
      new Configuration(username, host, port, Option(password), Option(database))
    }
    val connection = new MySQLConnection(configuration)
    Await.result(connection.connect, Duration.Inf)
    connection
  }

  def runQuery(sqlQuery: String): ResultSet = {
    val future: Future[QueryResult] = mysqlConnection
      .sendPreparedStatement(sqlQuery)
    Await.result(future, Duration.Inf).rows.get
  }
}
