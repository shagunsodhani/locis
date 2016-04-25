package com.github.locis.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
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

  private def isTableExist(tableName: TableName) = {
    admin.tableExists(tableName)
  }
  
  def createTable(tableName: String, columnList: List[String]) = {
    val _tableName = TableName.valueOf(tableName)
    if (!isTableExist(_tableName)) {
      val tableDescriptor = new HTableDescriptor(_tableName)
      columnList.foreach { x =>
        tableDescriptor.addFamily(new HColumnDescriptor(x))
      }
      admin.createTable(tableDescriptor)
    }
  }

  def deleteTable(tableName: String) = {
    val _tableName = TableName.valueOf(tableName)
    if (admin.tableExists(_tableName)) {
      admin.disableTable(_tableName)
      admin.deleteTable(_tableName)
    }
  }

}