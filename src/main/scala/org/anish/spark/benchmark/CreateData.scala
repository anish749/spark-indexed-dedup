package org.anish.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Random

/**
  * Create some data meaningful for benchmarking.
  *
  * Created by anish on 28/05/17.
  */
class CreateData(sqlContext: SQLContext,
                 partitionValues: List[String], // List of partition values for which data would be created
                 rowsPerPartition: Int) {

  import sqlContext.implicits._

  val partitionColumnName = "creation_date"

  val naturalKey = "log_id"

  val dataFiledNames = List("data1", "data2", "data3", "data4")

  def createDataFrame(): DataFrame = {
    partitionValues.map(_.toInt).map(partitionVal => createPartition.withColumn(partitionColumnName, lit(partitionVal)))
      .reduce(_.unionAll(_))
  }

  private def createPartition: DataFrame = {
    val df = sqlContext.sparkContext.parallelize(Seq.fill(rowsPerPartition)(Random.nextInt))
      .toDF(dataFiledNames.head)
    require(dataFiledNames.size > 1, "Not implemented for 1 column")

    // Copy the data to dataFiledNames.size columns
    dataFiledNames.tail
      .foldLeft(df) { (acc: DataFrame, x) => acc.withColumn(x, col(dataFiledNames.head)) }
      .withColumn(naturalKey, monotonicallyIncreasingId.cast(StringType))
  }

}
