package org.anish.spark.benchmark

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anish on 28/05/17.
  */
class SimpleJoinSQL(sparkMaster: String, inputExistingDataPath: String, inputIncrementalDataPath: String, outputLocation: String) {

  def deDupSQL: Unit = {
    val sparkConf = new SparkConf().setMaster(sparkMaster).setAppName("SimpleSQLBasedDedupApp")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val existing = sqlContext
      .read
      .load(inputExistingDataPath)

    val newData = sqlContext
      .read
      .load(inputIncrementalDataPath)

    //    val partitionColumn = "creation_date"
    //    val primaryKeys = Seq("log_id")

    existing.registerTempTable("existingMasterData")

    newData.registerTempTable("newDataFeed")

    // This is just to time. Values are hard coded
    sqlContext.sql("select distinct creation_date from (select * from newDataFeed) t")
      .show // Trigger action for benchmarking

    sqlContext.sql("select count(`data1`),count(`data2`),count(`data3`),count(`data4`),creation_date " +
      "from ( select lf.primarykey1,lf.`data1`,lf.`data2`,lf.`data3`,lf.`data4`, creation_date as creation_date " +
      "from (select *, log_id primarykey1 from newDataFeed) lf left outer join ( " +
      "select log_id primarykey1 from existingMasterData where (`creation_date` IN ('20170108', '20170109')) " +
      ") rt on lf.`primarykey1` <=> rt.`primarykey1` " +
      "where (rt.`primarykey1` is null)) t group by primarykey1, creation_date")
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(outputLocation)

    sparkContext.stop()
  }

}

