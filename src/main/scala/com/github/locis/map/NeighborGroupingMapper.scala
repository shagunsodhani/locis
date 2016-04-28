package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser

/*
 * This class maps the input data points to one of their neighbors.
 * See : https://github.com/shagunsodhani/locis/issues/6
 */

class NeighborGroupingMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context) = {
    val line = value.toString().split("\t")
    val keyString = line(0)
    val valueString = line(1)
    if (keyString == valueString || DataParser.getType(keyString) < DataParser.getType(valueString)) {
      context.write(new Text(keyString), new Text(valueString))
    }
  }
}
