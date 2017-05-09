package org.anish.spark.indexeddedup

import org.anish.spark.indexeddedup.estimator.{PartitionEstimator, SimpleDateBasedPartitionEstimator}
import org.anish.spark.{SimpleSparkSession, SparkTestUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.HashSet

/**
  * Created by anish on 07/05/17.
  */
class DeDuplicateMergeDataTest extends FlatSpec with Matchers {
  val newDataWithDupPath = SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/sampleData/newDataWithDup/")
  behavior of "DeDuplicateMergeData"

  // First load tests
  it should "return deduplicated new data without existing data on first load" in {
    val sparkContext = SimpleSparkSession.sparkContext
    val sqlContext = SimpleSparkSession.sqlContext
    import sqlContext.implicits._

    val firstLoadData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(newDataWithDupPath)
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val newDataPartitionEstimator: PartitionEstimator =
      new SimpleDateBasedPartitionEstimator(inputFilePath = newDataWithDupPath, datePattern = ".*(\\d{8}).*", sparkContext = sparkContext)

    val deDuplicateMergeData = new DeDuplicateMergeData(sparkContext, sqlContext)
    val (mergedData, mergedHashSet) = deDuplicateMergeData.runEtl(sqlContext.emptyDataFrame, firstLoadData, "undefined", "date", Seq("photo_id"), newDataPartitionEstimator)

    mergedData.cache()

    mergedData.rdd.partitions.length shouldBe 2
    mergedData.count shouldBe firstLoadData.distinct.count
    SparkTestUtils.dfEquals(mergedData, firstLoadData.distinct)

    val hashSetArray: Array[(Int, HashSet[Seq[String]])] = mergedHashSet.collect

    hashSetArray.length shouldBe 2
    hashSetArray.map(_._1) shouldBe firstLoadData.select("date").distinct.collect.map(_.getAs[Int]("date"))
    hashSetArray.map(_._2.size).forall(Seq(5, 7) contains _) shouldBe true
  }

  // HashSet is absent, but there is existing data
  it should "deduplicate new data before appending even if HashSet Index of old data is not present. " +
    "New HashSet should contain old as well as new Data" in {
    val sparkContext = SimpleSparkSession.sparkContext
    val sqlContext = SimpleSparkSession.sqlContext
    import sqlContext.implicits._

    val oldData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/sampleData/existingData/"))
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val newData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(newDataWithDupPath)
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val newDataPartitionEstimator: PartitionEstimator =
      new SimpleDateBasedPartitionEstimator(inputFilePath = newDataWithDupPath, datePattern = ".*(\\d{8}).*", sparkContext = sparkContext)

    val deDuplicateMergeData = new DeDuplicateMergeData(sparkContext, sqlContext)
    val (mergedData: DataFrame, mergedHashSet: RDD[(Int, HashSet[Seq[String]])]) =
      deDuplicateMergeData.runEtl(oldData, newData, "undefined", "date", Seq("photo_id"), newDataPartitionEstimator)

    mergedData.cache
    mergedHashSet.cache

    mergedData.rdd.partitions.length shouldBe 2

    // Examine the merged data per partition
    //    mergedData.rdd.mapPartitionsWithIndex((i, iter) => {
    //      println("Partition No " + i)
    //      println("\titerator as list = " + iter.toList)
    //      iter
    //    }).collect()

    SparkTestUtils.dfEquals(mergedData, newData.except(oldData).distinct)

    // Examine the Merged HashSet
    //    mergedHashSet.foreachPartition(iter => {
    //      val list = iter.toList
    //      if (list.nonEmpty) {
    //        val partitionKey = list.headOption.get._1
    //        val headValue = list.map(_._2).headOption.get
    //        println(s"partitionKey is $partitionKey, length is ${list.length} and head value is $headValue")
    //      }
    //    })

    mergedHashSetChecksForTestData(oldData, newData, mergedHashSet)
  }

  def mergedHashSetChecksForTestData(oldData: DataFrame,
                                     newData: DataFrame,
                                     mergedHashSet: RDD[(Int, HashSet[Seq[String]])]) = {
    val hashSetArray: Array[(Int, HashSet[Seq[String]])] = mergedHashSet.collect

    hashSetArray.length shouldBe 3
    hashSetArray.map(_._1).sorted shouldBe oldData.unionAll(newData).select("date").distinct.collect.map(_.getAs[Int]("date")).sorted
    hashSetArray.map(_._2.size).forall(Seq(7, 8) contains _) shouldBe true
  }

  // Usual success case
  it should "deduplicate the new data and merge with the old data based on the the HashSet Index" in {
    val sparkContext = SimpleSparkSession.sparkContext
    val sqlContext = SimpleSparkSession.sqlContext
    import sqlContext.implicits._

    val oldData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/sampleData/existingData/"))
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val newData = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(newDataWithDupPath)
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val newDataPartitionEstimator: PartitionEstimator =
      new SimpleDateBasedPartitionEstimator(inputFilePath = newDataWithDupPath, datePattern = ".*(\\d{8}).*", sparkContext = sparkContext)

    val deDuplicateMergeData = new DeDuplicateMergeData(sparkContext, sqlContext)

    // Simulate First load to create and save the HashSetRdd
    val (_, oldDataHashSetIndex) = deDuplicateMergeData.runEtl(sqlContext.emptyDataFrame, oldData, "undefined", "date", Seq("photo_id"), newDataPartitionEstimator)

    // Save the HashSet
    val ts = System.currentTimeMillis
    val tmpLocationForHashSetIndex = "/tmp/hashSetIndex_" + ts
    deDuplicateMergeData.overwriteHashSetRDD(tmpLocationForHashSetIndex, oldDataHashSetIndex)

    val (mergedData, mergedHashSet) = deDuplicateMergeData.runEtl(oldData, newData, tmpLocationForHashSetIndex, "date", Seq("photo_id"), newDataPartitionEstimator)

    mergedData.cache
    mergedHashSet.cache
    try {
      SparkTestUtils.dfEquals(mergedData, newData.except(oldData).distinct)
      mergedHashSetChecksForTestData(oldData, newData, mergedHashSet)
    }
    finally {
      // Clear the tmp path where the HashSet Index was stored
      val filesystemPath = new Path(tmpLocationForHashSetIndex)
      val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      if (fs.exists(filesystemPath))
        fs.delete(filesystemPath, true)
    }

  }
}
