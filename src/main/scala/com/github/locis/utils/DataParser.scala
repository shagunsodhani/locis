package com.github.locis.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object DataParser {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val sep = ","
  private val typeIndex = 1 //crime type
  private val xIndex = 2 //x coordinate
  private val yIndex = 3 //y coordinate

  def getType(dataPoint: String) = {
    dataPoint.split(sep)(typeIndex)
  }

  def getX(dataPoint: String): Double = {
    dataPoint.split(sep)(xIndex).toDouble
  }

  def getY(dataPoint: String): Double = {
    dataPoint.split(sep)(yIndex).toDouble
  }

}