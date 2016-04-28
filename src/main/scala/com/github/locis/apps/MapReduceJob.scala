package com.github.locis.apps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.GenericOptionsParser
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.ConfigUtils

abstract class MapReduceJob {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  protected val configuration = ConfigUtils.getHadoopConfiguration()

  def run(inputPath: Path, outputPath: Path)

  def main(args: Array[String]): Unit = {
    val otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: $HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.JOBNAME <inputFileName> <outputFileName>")
    } else {
      val inputPath = new Path(args(0))
      val outputPath = new Path(args(1))
      run(inputPath, outputPath)
    }

  }
}