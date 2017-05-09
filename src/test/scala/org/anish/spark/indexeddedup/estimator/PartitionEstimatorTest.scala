package org.anish.spark.indexeddedup.estimator

import org.anish.spark.{SimpleSparkSession, SparkTestUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by anish on 25/04/17.
  */
class PartitionEstimatorTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    sc = SimpleSparkSession.sparkContext
  }

  behavior of "Input Split based Partition Estimator"
  it should "return the number of partitions based on number of input files in path" in {
    val testInputPath = SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/estimator/sample-files")
    val estimator: PartitionEstimator = new InputSplitBasedPartitionEstimator(testInputPath, sc)

    estimator.estimate shouldBe 2
  }

  behavior of "Simple Date Based Partition Estimator"
  it should "return the number of required partitions based on each day" in {
    val testInputPath = SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/estimator/dated-sample-files")
    val datePattern: String = ".*(\\d{8}).*"
    val estimator: PartitionEstimator = new SimpleDateBasedPartitionEstimator(testInputPath, datePattern, sc)

    estimator.estimate shouldBe 4
  }
}
