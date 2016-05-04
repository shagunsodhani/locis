package com.github.locis.apps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import com.github.locis.map.CountInstanceMapper
import com.github.locis.reduce.CountInstanceReducer
import com.github.locis.utils.HBaseUtil

object CountInstance extends MapReduceJob {

  private val hBaseUtil = new HBaseUtil()

  def jobName: String = {
    "CountInstance"
  }

  def run(inputPath: Path, outputPath: Path): Unit = {
    hBaseUtil.createInstanceCountTable()
    val job = new Job(configuration, "Count Instance")
    job.setMapperClass(classOf[CountInstanceMapper])
    job.setReducerClass(classOf[CountInstanceReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass((classOf[LongWritable]))
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    val status = if (job.waitForCompletion(true)) 0 else 1
    System.exit(status)
  }
}