package com.github.locis.reduce

import scala.collection.JavaConversions.asScalaIterator

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.HBaseUtil

/*
 * This class counts the number of instances of each type and saves it to HBase
 * See : https://github.com/shagunsodhani/locis/issues/8
 */

class CountInstanceReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val hBaseUtil = new HBaseUtil()

  override def reduce(key: Text, values: java.lang.Iterable[LongWritable],
                      context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
    val sum = values.iterator().map { x => x.get() }.sum
    hBaseUtil.writeToInstanceCountTable(
      List(
        (key.toString(), sum.toString())))
    context.write(key, new LongWritable(sum))
  }

}