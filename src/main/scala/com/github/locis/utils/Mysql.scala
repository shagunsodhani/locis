package com.github.locis.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.typesafe.config.ConfigFactory
import scala.concurrent.{ Future, Await }
import com.github.mauricio.async.db.QueryResult
import scala.concurrent.duration.Duration

class Mysql {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private def createConfig: Configuration = {
    val username: String = ConfigFactory.load.getString("mysql.username")
    val host: String = ConfigFactory.load.getString("mysql.host")
    val port: Int = ConfigFactory.load.getInt("mysql.port")
    val password: String = ConfigFactory.load.getString("mysql.password")
    val database: String = ConfigFactory.load.getString("mysql.database")
    new Configuration(username, host, port, Option(password), Option(database))
  }

  def connect: com.github.mauricio.async.db.mysql.MySQLConnection = {
    val configuration = createConfig;
    new MySQLConnection(configuration);
  }
}

