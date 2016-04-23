package com.github.locis.apps

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.HDFSWriter
import com.github.locis.utils.Mysql

object DataLoader {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def getCountOfRows() = {
    val mysql = new Mysql()
    val sqlQuery = "SELECT count(*) FROM dataset"
    mysql.runQuery(sqlQuery)
  }

  def loadData(limit: Long = -1, start: Long = -1) = {
    //    Be careful with limit=-1. It would bring in a lot of data. (~500 mb)
    val mysql = new Mysql()
    val sqlQuery = {
      if (limit < 0) {
        "SELECT id, primary_type, xcoordinate, ycoordinate FROM dataset ORDER BY date ASC"
      } else if (start < 0) {
        "SELECT id, primary_type, xcoordinate, ycoordinate FROM dataset ORDER BY date ASC LIMIT " +
          limit.toString()
      } else {
        "SELECT id, primary_type, xcoordinate, ycoordinate FROM dataset ORDER BY date ASC LIMIT " +
          start.toString() + ", " + limit.toString()
      }
    }
    mysql.runQuery(sqlQuery)
  }

  def writeData(pathToWrite: String = "/user/locis/input/data", limit: Long = -1, start: Long = -1) = {
    val writer = new HDFSWriter().getWriter(pathToWrite)
    val result = loadData(limit, start)
    result.foreach { x => writer.write((x.mkString(",") + "\n").getBytes) }
    writer.close()
  }

  def writeAllData(pathToWrite: String = "/user/locis/input/data") = {
    val writer = new HDFSWriter().getWriter(pathToWrite)
    val start: Long = 0
    val limit: Long = 100000
    val countOfRows: Long = getCountOfRows()(0)(0).asInstanceOf[Long]
    val numberOfIterations = countOfRows / limit + 1
    (1 to numberOfIterations.asInstanceOf[Int]).foreach {
      iterationCounter =>
        {
          loadData(limit, start + limit * iterationCounter)
            .foreach { x => writer.write((x.mkString(",") + "\n").getBytes) }
        }
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val limit: Long = 10
    val pathToWrite = "/user/locis/input/data"
    writeAllData()
  }
}