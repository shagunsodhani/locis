package com.github.locis.map

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/*
 * This class maps the input data points to one of their neighbors.
 * See : https://github.com/shagunsodhani/locis/issues/6
 */

class NeighborGroupingMapper extends Mapper[Text, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  private val sep = ","
  private val typeIndex = 1

  private def getType(dataPoint: String) = {
    dataPoint.split(sep)(typeIndex)
  }
  override def map(key: Text, value: Text,
                   context: Mapper[Text, Text, Text, Text]#Context) = {
    val keyString = key.toString()
    val valueString = value.toString()
    if (keyString == valueString || getType(keyString) < getType(valueString)) {
      context.write(key, value)
    }
  }
}
