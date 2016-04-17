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

class CountInstanceMapper extends Mapper[Text, Seq[Text], Text, LongWritable] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def map(key: Text, value: Seq[Text],
                   context: Mapper[Text, Seq[Text], Text, LongWritable]#Context) = {
    context.write(new Text(DataParser.getType(key.toString())), new LongWritable(1L))
  }

}