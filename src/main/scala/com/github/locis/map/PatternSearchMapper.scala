package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser
import com.github.locis.utils.HBaseUtil

/*
 * This class maps the input data points to (eventset, instance).
 * See : https://github.com/shagunsodhani/locis/issues/7
 */

class PatternSearchMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val hBaseUtil = new HBaseUtil()

  private val internalSeprator = "#"
  //  used to separate records within a colocation instance

  private val externalSeperator = "\t"
  //  used to separate records in a list of colocations

  private def scanNTransactions(neighborhood: Array[String], k: Int): Iterator[Array[String]] = {
    val key = neighborhood(0)
    neighborhood
      .filterNot { _.equals(key) }
      .combinations(k - 1)
  }

  private def checkCliqueness(instance: Array[String], k: Int): Boolean = {
    if (instance.isEmpty) {
      true
    } else {
      val sortedInstance = instance
        .sortBy { dataPoint => DataParser.getType(dataPoint) }
      val rowName = sortedInstance
        .map { dataPoint => DataParser.getType(dataPoint) }
        .mkString(internalSeprator)

      hBaseUtil
        .readColocationStoreRow(rowName = rowName, size = k - 1)
        .split(externalSeperator)
        .contains(sortedInstance.mkString(internalSeprator))
    }
  }

  private def eventTypeOf(instance: Array[String]): Array[String] = {
    instance.map { DataParser.getType }
  }

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context) = {
    val k = context.getConfiguration.get("k").toInt
    val line = value.toString().split(externalSeperator)
    val keyString = line(0)

    /*
     *     I have a feeling that this could explode for very large datasets. It should not be brought in-memory.
     *     But querying HBase everytime would be very slow. Given the number of steps this set would take to be created, I am not convinced if
     *     filtering on basis of prevalent colocation types would indeed benefit the implementation.
     */
    val prevalentColocationTypes = {
      if (k > 1) {
        hBaseUtil
          .scanColocationStoreColumn("size", k - 1)
          .flatMap(colocationType => colocationType.split(internalSeprator).toIterable)
          .toSet
      } else {
        hBaseUtil
          .scanInstanceCountColumn("size")
          .flatMap(colocationType => colocationType.split(internalSeprator).toIterable)
          .toSet
      }
    }

    val neighborhood = line
      .tail
      .filter { neighbor => prevalentColocationTypes.contains(DataParser.getType(neighbor)) }

    scanNTransactions(neighborhood, k)
      .filter { checkCliqueness(_, k) }
      .foreach { instance =>
        {
          val eventSet = (eventTypeOf(instance) ++ Array(DataParser.getType(keyString)))
            .sortBy { identity }
            .mkString(internalSeprator)
          val sortedInstance = (instance ++ Array(keyString))
            .sortBy { dataPoint => DataParser.getType(dataPoint) }
            .mkString(internalSeprator)
          context.write(new Text(eventSet), new Text(sortedInstance))
        }
      }
  }
}