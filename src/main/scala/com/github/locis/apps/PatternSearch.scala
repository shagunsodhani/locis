package com.github.locis.apps

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import com.github.locis.map.PatternSearchMapper
import com.github.locis.reduce.PatternSearchReducer
import com.github.locis.utils.HBaseUtil

object PatternSearch extends MapReduceJob {

  private val hBaseUtil = new HBaseUtil()

  def jobName: String = {
    "PatternSearch"
  }

  override protected val errorMsg = "Usage: $HADOOP_HOME/bin/hadoop jar target/uber-locis-0.0.1-SNAPSHOT.jar " +
    "com.github.locis.apps." + jobName + " <inputFileName> <outputFileName> <size>"

  override protected val numberOfArgsExpected = 3

  def run(inputPath: Path, outputPath: Path, args: Array[String]): Unit = {
    val size = args(2)
    val thresholdParticipationIndex = userConfiguration.getString("participationIndex.threshold")
    hBaseUtil.createColocationStoreTable()
    hadoopConfiguration.set("k", size)
    hadoopConfiguration.set("thresholdParticipationIndex", thresholdParticipationIndex)
    val job = new Job(hadoopConfiguration, jobName)
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