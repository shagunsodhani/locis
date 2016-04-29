package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser

/*
 * This class maps the input data points to value 1.
 * See : https://github.com/shagunsodhani/locis/issues/8
 */

class CountInstanceMapper extends Mapper[LongWritable, Text, Text, LongWritable] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, LongWritable]#Context) = {
    val line = value.toString().split("\t")
    val keyString = line(0)
    context.write(new Text(DataParser.getType(keyString)), new LongWritable(1L))
  }

}