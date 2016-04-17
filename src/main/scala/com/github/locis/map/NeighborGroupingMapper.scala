package com.github.locis.map

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser

/*
 * This class maps the input data points to one of their neighbors.
 * See : https://github.com/shagunsodhani/locis/issues/6
 */

class NeighborGroupingMapper extends Mapper[Text, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def map(key: Text, value: Text,
                   context: Mapper[Text, Text, Text, Text]#Context) = {
    val keyString = key.toString()
    val valueString = value.toString()
    if (keyString == valueString || DataParser.getType(keyString) < DataParser.getType(valueString)) {
      context.write(key, value)
    }
  }
}
