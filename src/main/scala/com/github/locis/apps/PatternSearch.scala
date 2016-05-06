package com.github.locis.apps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.github.locis.map.PatternSearchMapper
import com.github.locis.reduce.PatternSearchReducer
import org.apache.hadoop.io.DoubleWritable
import com.github.locis.utils.HBaseUtil

object PatternSearch extends MapReduceJob {
  
  private val hBaseUtil = new HBaseUtil()

  def jobName: String = {
    "PatternSearch"
  }

  def run(inputPath: Path, outputPath: Path): Unit = {
    hBaseUtil.createColocationStoreTable()
    configuration.set("k", "1")
    configuration.set("thresholdParticipationIndex", "0")
    val job = new Job(configuration, "Neighborhood Grouping")
    job.setMapperClass(classOf[PatternSearchMapper])
    job.setReducerClass(classOf[PatternSearchReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass((classOf[Text]))
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])
    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    val status = if (job.waitForCompletion(true)) 0 else 1
    System.exit(status)
  }
}