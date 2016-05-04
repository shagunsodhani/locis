package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/*
 * This class maps the input data points to (eventset, instance).
 * See : https://github.com/shagunsodhani/locis/issues/7
 */

class PatternSearchMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private def isPrevalentType(neighbor: String): Boolean = {
    true
  }

  private def scanNTransactions(neighborhood: Array[String]): Array[String] = {
    neighborhood
  }

  private def checkCliqueness(instance: String): Boolean = {
    true
  }

  private def eventTypeOf(instance: String): String = {
    instance
  }
  private val k = 2

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context) = {
    val line = value.toString().split("\t")
    val keyString = line(0)
    val neighborhood = line
      .tail
      .filter { isPrevalentType }
    scanNTransactions(neighborhood)
      .filter { checkCliqueness }
      .foreach { instance =>
        {
          val eventSet = eventTypeOf(instance)
          context.write(new Text(eventSet), new Text(instance))
        }
      }
  }
}