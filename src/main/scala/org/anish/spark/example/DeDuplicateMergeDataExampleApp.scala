package org.anish.spark.example

import org.anish.spark.indexeddedup.DeDuplicateMergeData
import org.anish.spark.indexeddedup.estimator.{PartitionEstimator, SimpleDateBasedPartitionEstimator}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anish on 07/05/17.
  */
object DeDuplicateMergeDataExampleApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("HashTableBasedDedupApp")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val existing_tmp = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/smallData/alreadyExistingData/")
    val existing = existing_tmp.columns
      .foldLeft(existing_tmp)((acc_df: DataFrame, oldCol: String) => acc_df.withColumnRenamed(oldCol, oldCol.trim))
      .withColumn("partitionColumn", 'bday_year.cast(IntegerType))
      .cache()


    val newData_tmp = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/smallData/newIncrement/")
    val newData = newData_tmp.columns
      .foldLeft(newData_tmp)((acc_df: DataFrame, oldCol: String) => acc_df.withColumnRenamed(oldCol, oldCol.trim))
      .withColumn("partitionColumn", 'bday_year.cast(IntegerType))
      .cache()

    val hashSetIndexLocation = "hashSetIndex"
    val partitionColumn = "partitionColumn"
    val primaryKeys = Seq("member_id")
    val newDataPartitionEstimator: PartitionEstimator =
      new SimpleDateBasedPartitionEstimator(inputFilePath = "data/smallData/newIncrement/", datePattern = ".*(\\d{8}).*", sparkContext = sparkContext)

    val dataMerger = new DeDuplicateMergeData(sparkContext = sparkContext, sqlContext = sqlContext)
    val (mergedData, mergedHashSet) = dataMerger.runEtl(existing, newData, hashSetIndexLocation, partitionColumn, primaryKeys, newDataPartitionEstimator)

    println("merged data is")
    mergedData.show

    println("mergedDataHashset")
    mergedHashSet.foreachPartition(iter => {
      val list = iter.toList
      val partitionKey = list.headOption.getOrElse((-1, None))._1
      val headValue = list.map(_._2).headOption.getOrElse(None)
      //      println(s"partitionKey is $partitionKey, and value is $hashSet")
      println(s"partitionKey is $partitionKey, length is ${list.length} and head value is $headValue")
    })

  }
}
