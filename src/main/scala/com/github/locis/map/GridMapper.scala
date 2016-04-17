package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/*
 * This class maps the input data points to the grid number. For now, we are 
 * using a dummy implementation. 
 * See : https://github.com/shagunsodhani/locis/issues/2
 */

class GridMapper extends Mapper[LongWritable, Text, LongWritable, Text] {

  private def getGridNumber(dataPoint: String): Long = {
    1L
  }
  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, LongWritable, Text]#Context) = {
    val gridNumber = new LongWritable(getGridNumber(value.toString()))
    context.write(gridNumber, value)
  }

}
