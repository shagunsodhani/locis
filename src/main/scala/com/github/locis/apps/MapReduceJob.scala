package com.github.locis.apps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.GenericOptionsParser
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.github.locis.utils.ConfigUtils
import com.typesafe.config.ConfigFactory

abstract class MapReduceJob {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  protected val hadoopConfiguration = ConfigUtils.getHadoopConfiguration()

  protected val userConfiguration = ConfigFactory.load

  def jobName(): String

  protected val errorMsg = "Usage: $HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar " +
    "com.github.locis.apps." + jobName + " <inputFileName> <outputFileName>"

  protected val numberOfArgsExpected = 2

  def run(inputPath: Path, outputPath: Path, args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    val otherArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs
    if (otherArgs.length != numberOfArgsExpected) {
      println(errorMsg)
    } else {
      val inputPath = new Path(args(0))
      val outputPath = new Path(args(1))
      run(inputPath, outputPath, args)
    }

  }
}