package com.epam.training.spark.core

import java.time.LocalDate

import com.epam.training.spark.core.domain.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing list in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {
    val rows = sc.textFile(rawDataPath)
    val dataRows = rows.mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
    dataRows.map(line => line.split(";", 7).map(_.trim).toList)
  }

  def findErrors(rawData: RDD[List[String]]): List[Int] =
    rawData.map(missingValues).reduce(addLists)

  private def missingValues(list: List[String]): List[Int] =
    list.map(value => if (value == "") 1 else 0)

  private def addLists(list1: List[Int], list2: List[Int]): List[Int] =
    list1.zipAll(list2, 0, 0).map { case (value1, value2) => value1 + value2 }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] =
    rawData.map(dataRow => Climate(dataRow(0), dataRow(1), dataRow(2), dataRow(3), dataRow(4), dataRow(5), dataRow(6)))

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] =
    climateData
      .filter(climate => isMonthAndDayEqual(climate.observationDate, month, dayOfMonth))
      .map(_.meanTemperature.value)

  //  def predictTemperature_1(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
  //    val filtered = climateData.filter(climate => isMonthAndDayEqualOrAdjacent(climate.observationDate, month, dayOfMonth))
  //    val count = filtered.count
  //    val sum = filtered.map(_.meanTemperature.value).sum
  //    sum / count
  //  }


  //  def predictTemperature_2(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
  //    val (sum, count) = climateData
  //      .filter(climate => isMonthAndDayEqualOrAdjacent(climate.observationDate, month, dayOfMonth))
  //      .map(_.meanTemperature.value)
  //      .aggregate((0.0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  //    sum / count
  //  }

//  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
//    val (sum, count) = climateData
//      .filter(climate => isMonthAndDayEqualOrAdjacent(climate.observationDate, month, dayOfMonth))
//      .map(data => (data.meanTemperature.value, 1))
//      .reduce(addTuples)
//    sum / count
//  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    climateData
      .filter(climate => isMonthAndDayEqualOrAdjacent(climate.observationDate, month, dayOfMonth))
      .map(data => data.meanTemperature.value)
      .mean()
  }

//  private def addTuples(tuple1: (Double, Int), tuple2: (Double, Int)): (Double, Int) =
//    (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)

  private def isMonthAndDayEqual(date: LocalDate, month: Int, dayOfMonth: Int): Boolean =
    date.getMonth.getValue == month && date.getDayOfMonth == dayOfMonth

  private def isMonthAndDayEqualOrAdjacent(date: LocalDate, month: Int, dayOfMonth: Int): Boolean =
    isMonthAndDayEqual(date, month, dayOfMonth) || isMonthAndDayAdjacent(date, month, dayOfMonth)

  private def isMonthAndDayAdjacent(date: LocalDate, month: Int, dayOfMonth: Int): Boolean = {
    val previousDate = date.minusDays(1)
    val nextDate = date.plusDays(1)
    isMonthAndDayEqual(previousDate, month, dayOfMonth) || isMonthAndDayEqual(nextDate, month, dayOfMonth)
  }

}


