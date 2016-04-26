package com.github.locis.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.map.NeighborSearchMapper
import com.github.locis.reduce.NeighborSearchReducer
import com.typesafe.config.ConfigFactory

object NeighborSearch {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", ConfigFactory.load.getString("fs.defaultFS"))
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

    val otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: $HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar com.github.locis.apps.NeighborSearch <inputFileName> <outputFileName>")
    } else {
      val job = new Job(configuration, "Neighborhood Search")
      job.setMapperClass(classOf[NeighborSearchMapper])
      job.setReducerClass(classOf[NeighborSearchReducer])
      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass((classOf[Text]))
      job.setOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path((args(1))))
      val status = if (job.waitForCompletion(true)) 0 else 1
      System.exit(status)
    }
  }  
}