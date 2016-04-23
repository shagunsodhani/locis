package com.github.locis.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object DistanceMeasure {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val earthRadius: Double = 6371 //in km

  def haversineDistance(lat1: Double, lng1: Double, lat2: Double, lng2: Double) = {
    /*
 * Function to return distance between two points (represented by latitude and longitude).
 * The distance is returned in km
 */
    val dLat = Math.toRadians(lat2 - lat1)
    val dLng = Math.toRadians(lng2 - lng1)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    earthRadius * c
  }

  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double) = {
    /*
 * Function to return distance between two points in the euclidean space.
 * The unit is not known but can be worked out.
 */
    math.sqrt(math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2))
  }

}
