package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.github.locis.utils.DataParser

/*
 * This class maps the input data points to (eventset, instance).
 * See : https://github.com/shagunsodhani/locis/issues/7
 */

class PatternSearchMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val sep = "#"

  private def isPrevalentType(neighbor: String, k: Int): Boolean = {
    if (k == 1) {
      true
    } else {
      //     To be replaced by HBase logic
      true
    }
  }

  private def scanNTransactions(neighborhood: Array[String], k: Int): Iterator[Array[String]] = {
    val key = neighborhood(0)
    neighborhood
    .filterNot { _.equals(key) }
    .combinations(k - 1)
  }

  private def checkCliqueness(instance: Array[String]): Boolean = {
    if (instance.isEmpty) {
      true
    } else {
      //      To be replaced by HBase logic
      true
    }
  }

  private def eventTypeOf(instance: Array[String]): Array[String] = {
    instance.map { DataParser.getType }
  }

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context) = {
    val k = context.getConfiguration.get("k").toInt
    val line = value.toString().split("\t")
    val keyString = line(0)
    val neighborhood = line
      .tail
      .filter { neighbor => isPrevalentType(neighbor, k) }

    scanNTransactions(neighborhood, k)
      .filter { checkCliqueness }
      .foreach { instance =>
        {
          val eventSet = (eventTypeOf(instance) ++ Array(DataParser.getType(keyString)))
            .sortBy { dataPoint => DataParser.getType(dataPoint) }
            .mkString(sep)
          val sortedInstance = (instance ++ Array(keyString))
            .sortBy { dataPoint => DataParser.getType(dataPoint) }
            .mkString(sep)
          context.write(new Text(eventSet), new Text(instance.mkString(sep)))
        }
      }
  }
}