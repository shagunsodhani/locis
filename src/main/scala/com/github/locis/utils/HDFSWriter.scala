package com.github.locis.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.util.Progressable
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.typesafe.config.ConfigFactory

class HDFSWriter {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val configuration = new Configuration()
  configuration.set("fs.defaultFS", ConfigFactory.load.getString("fs.defaultFS"))
  configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
  configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

  private val fileSystem = FileSystem.get(configuration)

  def getWriter(outputFilePath: String) = {
    val progressable = new Progressable() {
      @Override
      def progress() {
        print(".");
      }
    }
    fileSystem.create(new Path(outputFilePath), progressable)
  }
}