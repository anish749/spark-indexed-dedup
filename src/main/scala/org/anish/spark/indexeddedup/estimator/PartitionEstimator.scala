package org.anish.spark.indexeddedup.estimator

import org.apache.spark.SparkContext

/**
  * An object to estimate the number of partitions that should be needed for optimal performance of most Spark jobs.
  *
  * Created by anish on 22/04/17.
  */
abstract class PartitionEstimator extends Serializable {
  def estimate: Int
}

/**
  * File path based Partition Estimator. Based on directories groups 24 hourly files into one partition
  *
  * datePattern should be a valid RegEx. It is parsed to scala.util.matching. Regex inside the function since the Regex
  * class is not Serializable
  */
class SimpleDateBasedPartitionEstimator(inputFilePath: String, datePattern: String, sparkContext: SparkContext) extends PartitionEstimator {

  override def equals(other: Any): Boolean = other match {
    case e: SimpleDateBasedPartitionEstimator =>
      e.estimate == estimate
    case _ =>
      false
  }

  lazy val estimate: Int = {
    val datePatternRegex = datePattern.r
    val datesInFiles = sparkContext
      .wholeTextFiles(inputFilePath)
      .map(_._1)
      .map {
        case datePatternRegex(date) => date
        case _ => println("ERROR : did not match given regex")
      }
      .distinct
      .collect
      .toList

    datesInFiles.length
  }

  override def hashCode: Int = estimate
}

/**
  * Exact number of partitions based on input files. Assumed that all input files will have a similar number of records.
  * It doesn't do any Sampling to arrive at this value.
  */
class InputSplitBasedPartitionEstimator(inputFilePath: String, sparkContext: SparkContext) extends PartitionEstimator {

  override def equals(other: Any): Boolean = other match {
    case e: InputSplitBasedPartitionEstimator =>
      e.estimate == estimate
    case _ =>
      false
  }

  lazy val estimate: Int = sparkContext
    .wholeTextFiles(inputFilePath)
    .map(_._1)
    .count()
    .toInt

  override def hashCode: Int = estimate
}