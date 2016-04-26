package com.github.locis.map

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.github.locis.utils.DataParser

/*
 * This class maps the input data points to the grid number. For now, we are 
 * using a dummy implementation. 
 * See : https://github.com/shagunsodhani/locis/issues/2
 */

class NeighborSearchMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  private def getGridId(dataPoint: String): String = {
    DataParser.getKeyForGridMapping(dataPoint)
  }
  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context) = {
    val gridNumber = new Text(getGridId(value.toString()))
    context.write(gridNumber, value)
  }

}
