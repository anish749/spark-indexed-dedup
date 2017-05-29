package org.anish.spark.benchmark

import org.anish.spark.indexeddedup.DeDuplicateMergeData
import org.anish.spark.indexeddedup.estimator.{PartitionEstimator, SimpleDateBasedPartitionEstimator}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anish on 28/05/17.
  */
class IndexedDedup(sparkMaster: String, inputExistingDataPath: String, inputIncrementalDataPath: String, hashSetIndexLocation: String, outputLocation: String) {
  def doDedup: Unit = {
    val sparkConf = new SparkConf().setMaster(sparkMaster).setAppName("HashTableBasedDedupApp")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val existing = if (inputExistingDataPath == null)
      sqlContext.emptyDataFrame
    else
      sqlContext
        .read
        .load(inputExistingDataPath)
        .cache()

    val newData = sqlContext
      .read
      .load(inputIncrementalDataPath)
      .cache()

    val partitionColumn = "creation_date"
    val primaryKeys = Seq("log_id")
    val newDataPartitionEstimator: PartitionEstimator =
      new SimpleDateBasedPartitionEstimator(inputFilePath = inputIncrementalDataPath, datePattern = ".*(\\d{8}).*", sparkContext = sparkContext)

    val dataMerger = new DeDuplicateMergeData(sparkContext = sparkContext, sqlContext = sqlContext)
    val (mergedData, mergedHashSet) = dataMerger.runEtl(existing, newData, hashSetIndexLocation, partitionColumn, primaryKeys, newDataPartitionEstimator)

    // The merged data is cached for future use
    mergedData.cache.write.mode(SaveMode.Overwrite).partitionBy(partitionColumn).save(outputLocation)

    //    println("mergedDataHashset")
    //    mergedHashSet.foreachPartition(iter => {
    //      val list = iter.toList
    //      val partitionKey = list.headOption.getOrElse((-1, None))._1
    //      val headValue = list.map(_._2).headOption.getOrElse(None)
    //      //      println(s"partitionKey is $partitionKey, and value is $hashSet")
    //      println(s"partitionKey is $partitionKey, length is ${list.length} and head value is $headValue")
    //    })

    // Gracefully overwrite
    dataMerger.overwriteHashSetRDD(hashSetIndexLocation, mergedHashSet)
    sparkContext.stop()
  }
}
