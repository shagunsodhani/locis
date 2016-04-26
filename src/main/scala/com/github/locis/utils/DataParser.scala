package com.github.locis.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object DataParser {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val sep = ","

  private val attributeList: List[String] = List(
    "id",
    "primary_type",
    "x_coordinate",
    "y_coordinate",
    "longitude",
    "latitude",
    "district",
    "ward",
    "community_area",
    "fbi_code")

  private val attributeIndexMap = attributeList.zipWithIndex.toMap

  private def getIndex(attribute: String) = {
    attributeIndexMap.get(attribute)
  }

  def getType(dataPoint: String): String = {
    val typeIndex = attributeIndexMap("primary_type")
    dataPoint.split(sep)(typeIndex)
  }

  def getX(dataPoint: String): Double = {
    val xIndex = attributeIndexMap("x_coordinate")
    dataPoint.split(sep)(xIndex).toDouble
  }

  def getY(dataPoint: String): Double = {
    val yIndex = attributeIndexMap("y_coordinate")
    dataPoint.split(sep)(yIndex).toDouble
  }

  def getAttributeList: List[String] = {
    attributeList
  }

  def getKeyForGridMapping(dataPoint: String): String = {
    val keyIndex = attributeIndexMap("district")
    dataPoint.split(sep)(keyIndex)
  }
}