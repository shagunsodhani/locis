package com.github.locis.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.typesafe.config.ConfigFactory

object ConfigUtils {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def getHadoopConfiguration() = {
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", ConfigFactory.load.getString("fs.defaultFS"))
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    configuration
  }

}