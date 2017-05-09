package org.anish.spark.indexeddedup.internal

import org.anish.spark.indexeddedup.partitioner.SimpleModuloPartitioner
import org.anish.spark.{SimpleSparkSession, SparkTestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by anish on 25/04/17.
  */
class DeDupPairRDDTest extends FlatSpec with Matchers {
  val newDataWithDupPath = SparkTestUtils.getResourcePath("/org/anish/spark/indexeddedup/sampleData/newDataWithDup/")

  behavior of "DeDupPairRDD class"
  it should "Deduplicate a pair RDD based on the given columns" in {
    val sqlContext = SimpleSparkSession.sqlContext
    import sqlContext.implicits._

    val withDupDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(newDataWithDupPath)
      .withColumn("date", 'date.cast(IntegerType))
      .cache

    val partitioner = new SimpleModuloPartitioner[Int, Row](withDupDf.select("date").distinct.count.toInt)

    val dupPairRDD = withDupDf.rdd.keyBy(row => row.getAs[Int]("date")).partitionBy(partitioner)

    val deDupedPairRDD = new DeDupPairRDD[Int, Row](dupPairRDD, Array("photo_id"))

    val deDupedRDD = deDupedPairRDD.map(_._2) // Convert back to normal RDD

    val deDupedDf = sqlContext.createDataFrame(deDupedRDD, withDupDf.schema)

    SparkTestUtils.dfEquals(deDupedDf, withDupDf.distinct)
  }

}
