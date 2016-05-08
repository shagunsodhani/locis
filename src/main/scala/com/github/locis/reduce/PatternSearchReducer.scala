package com.github.locis.reduce

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.github.locis.utils.DataParser
import com.github.locis.utils.HBaseUtil
/*
 * This class groups all the instances for a given key (eventset). 
 * See : https://github.com/shagunsodhani/locis/issues/7
 */

class PatternSearchReducer extends Reducer[Text, Text, Text, DoubleWritable] {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val hBaseUtil = new HBaseUtil()
  private val internalSeprator = "#"
  //  used to separate records within a colocation instance

  private val externalSeperator = "\t"
  //  used to separate records in a list of colocations

  private def getEventTypeCount(eventType: String): Long = {
    hBaseUtil.readInstanceCountRow(eventType).toLong
  }
  private def computeParticipationIndexAndInstanceString(eventType: String,
                                                         instanceListIterator: Iterable[String]) = {
    /*
     * We have to compute the instance string in this function because we can not consume the iterable twice.
     */
    val eventTypeList = eventType.split(internalSeprator)
    val eventTypeToIntMapping = eventTypeList.zipWithIndex
    //    Mapping each event type to an integer
    val eventTypeToCountMapping = eventTypeToIntMapping.map {
      eventTypeIntTuple =>
        {
          (eventTypeIntTuple._2, getEventTypeCount(eventTypeIntTuple._1))
        }
    }.toMap
    //    Mapping each event type (via an integer) to a its total number of instances
    val eventTypeToSetMapping = eventTypeToIntMapping.map {
      eventTypeIntTuple =>
        {
          (eventTypeIntTuple._2, scala.collection.mutable.Set[String]())
        }
    }.toMap
    //      new scala.collection.mutable.HashMap[Int, scala.collection.mutable.Set[String]]
    //    Mapping each event type (via an integer) to a set of unique instances

    val instanceStringBuffer = new StringBuilder

    instanceListIterator.foreach { instanceList =>
      {
        val instancesToIntMapping = instanceList.split(internalSeprator).zipWithIndex
        instancesToIntMapping.foreach {
          instanceIntTuple =>
            {
              eventTypeToSetMapping(instanceIntTuple._2) += DataParser.getId(instanceIntTuple._1)
              instanceStringBuffer ++= instanceIntTuple._1 + internalSeprator
            }
        }
        instanceStringBuffer.deleteCharAt(instanceStringBuffer.length-1)
        instanceStringBuffer ++= externalSeperator
      }
    }
    instanceStringBuffer.deleteCharAt(instanceStringBuffer.length-1)
    val participationIndex = eventTypeToSetMapping.map {
      intSetTuple =>
        {
          intSetTuple._2.count { x => true }.toDouble / eventTypeToCountMapping(intSetTuple._1)
        }
    }.min
    (participationIndex, instanceStringBuffer.toString())
  }

  override def reduce(key: Text, values: java.lang.Iterable[Text],
                      context: Reducer[Text, Text, Text, DoubleWritable]#Context): Unit = {
    val k = context.getConfiguration.get("k").toInt
    val eventType = key.toString()
    val instanceListIterator = values.asScala.map { _.toString() }
    val thresholdParticipationIndex = context.getConfiguration.get("thresholdParticipationIndex").toDouble
    val (participationIndex, instanceString) = computeParticipationIndexAndInstanceString(eventType,
      instanceListIterator)
    if (participationIndex > thresholdParticipationIndex) {
      hBaseUtil.writeToColocationStoreTable(List((eventType, instanceString)), k)
      context.write(key, new DoubleWritable(participationIndex))
    }
  }
}