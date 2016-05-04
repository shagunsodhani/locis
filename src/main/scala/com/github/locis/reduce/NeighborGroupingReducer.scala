package com.github.locis.reduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser

/*
 * This class groups all the neighbors for a given key (dataPoint) in sorted order. 
 * algorithm. 
 * See : https://github.com/shagunsodhani/locis/issues/6
 */

class NeighborGroupingReducer extends Reducer[Text, Text, Text, Text] {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def reduce(key: Text, values: java.lang.Iterable[Text],
                      context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val dataPointIterator = values.iterator()
    val sortedObjectSet = {
      var tempObjectSet = Set[String]()
      while (dataPointIterator.hasNext()) {
        tempObjectSet += dataPointIterator.next().toString()
      }
      tempObjectSet.toSeq.sortBy { dataPoint => DataParser.getType(dataPoint) }
    }
//    val nRecord = (Seq(key.toString()) ++ sortedObjectSet).mkString("\t")
//    This step adds the key to the nRecord set for the second time which seems not only unnecessary but also erroneous.
    val nRecord = sortedObjectSet.mkString("\t")
    context.write(key, new Text(nRecord))
  }
}