package org.anish.spark.benchmark

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anish on 28/05/17.
  */
object Benchmark {

//  val hdfsPathPrefix = "/data/cdp2/benchmarks/"
    val hdfsPathPrefix = "tempOutput/"

  val recordsPerPartition = 1e1.toInt

  val existingDataPath = hdfsPathPrefix + "ramdomExistingData"
  val ramdomIncrementalDataWithDups = hdfsPathPrefix + "randomIncrementalDataWithDups"
  val hashSetIndexLocation = hdfsPathPrefix + "tmpHashSetPath"
  val outputLocationIndexed = hdfsPathPrefix + "mergedOutput_Indexed"
  val outputLocationSimpleJoin = hdfsPathPrefix + "mergedOutput_SimpleJoin"

  def main(args: Array[String]): Unit = {

    val sparkMaster = args(0)

    val sparkConf = new SparkConf().setMaster(sparkMaster).setAppName("BenchMarkApp")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    createDataFramesAndWriteToDisk(sqlContext)
    sparkContext.stop()

    //    time("Indexded Dedup (Load - createHashMap - Merge using HM)") {
    //      new IndexedDedup(
    //        sparkMaster = sparkMaster,
    //        inputExistingDataPath = existingDataPath,
    //        inputIncrementalDataPath = ramdomIncrementalDataWithDups,
    //        hashSetIndexLocation,
    //        outputLocation = outputLocationIndexed)
    //        .doDedup
    //    }

//    time("Indexded Dedup - First Load - (Create HashMap)") { // 44572466066 ns or 44572.466066 ms for 1e5 - yarn 106
//      new IndexedDedup(
//        sparkMaster = sparkMaster,
//        inputExistingDataPath = null,
//        inputIncrementalDataPath = existingDataPath,
//        hashSetIndexLocation,
//        outputLocation = outputLocationIndexed + "_firstload")
//        .doDedup
//    }
//
//    time("Indexded Dedup - Incremental Load - (Merge using HashMap)") { // 35690855950 ns or 35690.85595 ms for 1e5 - yarn 107
//      new IndexedDedup(
//        sparkMaster = sparkMaster,
//        inputExistingDataPath = outputLocationIndexed + "_firstload",
//        inputIncrementalDataPath = ramdomIncrementalDataWithDups,
//        hashSetIndexLocation,
//        outputLocation = outputLocationIndexed + "_secondload")
//        .doDedup
//    }

    time("Simple SQL Join") { //  263657031370 ns or 263657.03137 ms for 1e5 - yarn 108
      new SimpleJoinSQL(
        sparkMaster = sparkMaster,
        inputExistingDataPath = existingDataPath,
        inputIncrementalDataPath = ramdomIncrementalDataWithDups,
        outputLocation = outputLocationSimpleJoin)
        .deDupSQL
    }

    deleteDataFrames(new SparkContext(sparkConf))
  }

  def createDataFramesAndWriteToDisk(sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val existingData = new CreateData(sqlContext,
      List("20170101", "20170102", "20170103", "20170104", "20170105", "20170106", "20170107", "20170108"),
      recordsPerPartition).createDataFrame()

    val incrementData = new CreateData(sqlContext,
      List("20170109"),
      recordsPerPartition).createDataFrame()
      // Add a partition from previous already existing data
      .unionAll(existingData.where('creation_date === "20170108"))

    existingData.write.mode(SaveMode.Overwrite).partitionBy("creation_date").save(existingDataPath)
    incrementData.write
      .mode(SaveMode.Overwrite).partitionBy("creation_date").save(ramdomIncrementalDataWithDups)
  }

  def deleteDataFrames(sparkContext: SparkContext) = {
    val fs = FileSystem.get(new URI(hdfsPathPrefix), sparkContext.hadoopConfiguration)
    if (!fs.delete(new Path(hdfsPathPrefix), true))
      throw new RuntimeException(s"File deletion failed at $hdfsPathPrefix")
  }

  def time[R](blockName: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val timeElapsedNano = System.nanoTime() - t0
    println(s"Elapsed time for $blockName : $timeElapsedNano ns or ${timeElapsedNano / 1e6} ms")
    result
  }
}

/*

nohup \
spark-submit --master yarn-client \
--conf spark.storage.memoryFraction=0.3 \
--packages com.databricks:spark-csv_2.10:1.5.0 \
--driver-memory 32g \
--executor-memory 14g --executor-cores 3 --num-executors 8 \
--class org.anish.spark.benchmark.Benchmark spark-indexed-dedup-1.0-SNAPSHOT.jar yarn-client \
&

*/