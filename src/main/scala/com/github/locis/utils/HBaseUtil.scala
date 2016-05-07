package com.github.locis.utils

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class HBaseUtil {

  /*
 * This class contains all the HBase related logic. 
 * See : https://github.com/shagunsodhani/locis/issues/10
 * 		 : https://github.com/shagunsodhani/locis/issues/12	
 */

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val conf = HBaseConfiguration.create()
  private val connection = ConnectionFactory.createConnection(conf)
  private val admin = connection.getAdmin()
  private val instanceCountTableName = "InstanceCount"
  private val colocationStoreTableName = "ColocationStore"

  private def isTableExist(tableName: TableName) = {
    admin.tableExists(tableName)
  }

  private def createTable(tableName: String, columnList: List[String]) = {
    /*
 * This method creates a table with a given name.
 * If a table with same name exists, nothing is done.	
 */
    val _tableName = TableName.valueOf(tableName)
    if (!isTableExist(_tableName)) {
      val tableDescriptor = new HTableDescriptor(_tableName)
      columnList.foreach { x =>
        tableDescriptor.addFamily(new HColumnDescriptor(x))
      }
      admin.createTable(tableDescriptor)
    }
  }

  def createInstanceCountTable() = {
    /*
 * This method creates a table to track the instance counts for each event type.
 * For Data Model, see : https://github.com/shagunsodhani/locis/blob/master/docs/implementation.md#using-hbase-data-model
 */
    val columnList = List("size")
    createTable(instanceCountTableName, columnList)
  }

  def createColocationStoreTable() = {
    /*
 * This method creates a table to store colocations of different sizes.
 * For Data Model, see : https://github.com/shagunsodhani/locis/blob/master/docs/implementation.md#using-hbase-data-model
 */
    val columnList = List("size")
    createTable(colocationStoreTableName, columnList)
  }

  def writeToInstanceCountTable(rowkeyValueList: List[(String, String)]) = {
    val table = new HTable(conf, instanceCountTableName)
    val sizeByteArray = Bytes.toBytes("1")
    rowkeyValueList.foreach {
      rowkeyValue =>
        {
          val rowKey = rowkeyValue._1
          val value = rowkeyValue._2
          val row = new Put(Bytes.toBytes(rowKey))
          row.addImmutable(Bytes.toBytes("size"), sizeByteArray,
            Bytes.toBytes(value))
          table.put(row)
        }
        table.flushCommits()
        table.close()
    }
  }

  def writeToColocationStoreTable(rowkeyValueList: List[(String, String)],
                                  size: Int) = {
    val table = new HTable(conf, colocationStoreTableName)
    val sizeByteArray = Bytes.toBytes(size.toString())
    rowkeyValueList.foreach {
      rowkeyValue =>
        {
          val rowKey = rowkeyValue._1
          val value = rowkeyValue._2
          val row = new Put(Bytes.toBytes(rowKey))
          row.addImmutable(Bytes.toBytes("size"), sizeByteArray,
            Bytes.toBytes(value))
          table.put(row)
        }
        table.flushCommits()
        table.close()
    }
  }

  def deleteTable(tableName: String) = {
    val _tableName = TableName.valueOf(tableName)
    if (admin.tableExists(_tableName)) {
      admin.disableTable(_tableName)
      admin.deleteTable(_tableName)
    }
  }

  def scanColocationStoreColumn(columnName: String, size: Int): Iterable[String] = {
    /*
     * Method to scan a column in HBase.
     * See : https://github.com/shagunsodhani/locis/issues/15
     */
    val colocationStoreTable = new HTable(conf, colocationStoreTableName)
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(size.toString()))
    colocationStoreTable
      .getScanner(scan)
      .asScala
      .map { result => Bytes.toString(result.getRow) }
  }

  def readColocationStoreRow(rowName: String, size: Int): String = {
    /*
     * Method to read a row in HBase.
     * See : https://github.com/shagunsodhani/locis/issues/15
     */
    val colocationStoreTable = new HTable(conf, colocationStoreTableName)
    val get = new Get(Bytes.toBytes(rowName))
    val result: Result = colocationStoreTable.get(get)
    Bytes.toString(result.getValue(Bytes.toBytes("size"), Bytes.toBytes((size).toString())))
  }
}