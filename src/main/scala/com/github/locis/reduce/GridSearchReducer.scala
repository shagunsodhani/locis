package com.github.locis.reduce

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import com.typesafe.config.ConfigFactory
import com.github.locis.utils.DistanceMeasure
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/*
 * This class finds all the neighbors in a given grid using the plane-sweep 
 * algorithm. 
 * See : https://github.com/shagunsodhani/locis/issues/3
 */

class GridSearchReducer extends Reducer[LongWritable, Text, Text, Text] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val distanceThreshold: Double = ConfigFactory.load.getDouble("distance.threshold")

  private val sep = ","
  private val xIndex = 2 //longitude
  private val yIndex = 3 //latitude

  private def getX(dataPoint: String): Double = {
    dataPoint.split(sep)(xIndex).toDouble
  }

  private def getY(dataPoint: String): Double = {
    dataPoint.split(sep)(yIndex).toDouble
  }

  private def getDistance(dataPoint1: String, dataPoint2: String): Double = {
    val lat1 = getY(dataPoint1)
    val lat2 = getY(dataPoint2)

    val lng1 = getX(dataPoint1)
    val lng2 = getX(dataPoint2)

    DistanceMeasure.haversineDistance(lat1, lng1, lat2, lng2)
  }

  private def planeSweep(dataPoints: java.lang.Iterable[Text]): Set[(String, String)] = {
    val dataPointIterator = dataPoints.iterator()
    val objectSet = {
      var tempObjectSet = Set[String]()
      while (dataPointIterator.hasNext()) {
        tempObjectSet += dataPointIterator.next().toString()
      }
      tempObjectSet.toSeq.sortBy { dataPoint => getX(dataPoint) }
    }
    //    sorted Object Set
    var activeSet = Set[String]()
    var resultSet = Set[(String, String)]()
    var j = 1
    val n = objectSet.length
    (1 to n).foreach {
      i =>
        {
          while (getX(objectSet(i)) - getX(objectSet(j)) > distanceThreshold) {
            activeSet -= objectSet(i)
            j += 1
          }
          val range = activeSet
            .filter { dataPoint =>
              {
                val yDataPoint = getY(dataPoint)
                val yObjectSetPoint = getY(objectSet(i))
                ((yDataPoint <= yObjectSetPoint + distanceThreshold)
                  && (yDataPoint >= yObjectSetPoint - distanceThreshold))
              }
            }
          range.filter {
            dataPointInrange => (getDistance(objectSet(i), dataPointInrange) <= distanceThreshold)
          }.foreach { dataPoint => (resultSet += ((objectSet(i), dataPoint))) }
          activeSet += objectSet(i)
          resultSet += ((objectSet(i), objectSet(i)))
        }
    }
    resultSet
  }

  override def reduce(key: LongWritable, values: java.lang.Iterable[Text],
                      context: Reducer[LongWritable, Text, Text, Text]#Context): Unit = {
    planeSweep(values).foreach { dataPoint =>
      {
        context.write(new Text(dataPoint._1), new Text(dataPoint._2))
      }
    }

  }
}