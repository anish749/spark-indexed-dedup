package org.anish.spark.indexeddedup

import org.anish.spark.indexeddedup.estimator.PartitionEstimator
import org.anish.spark.indexeddedup.internal.{DeDupPairRDD, SerializableHelpers}
import org.anish.spark.indexeddedup.partitioner.SimpleModuloPartitioner
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.immutable.HashSet
import scala.util.{Failure, Success, Try}

/**
  * Created by anish on 17/04/17.
  */
class DeDuplicateMergeData(sparkContext: SparkContext, sqlContext: SQLContext) {

  /**
    * Steps in this ETL.
    * 1 Partitioning and deduplicating new data
    *   - Prepare (make a pair rdd) and partition (spark memory partition) the new data.
    *   - Remove duplicates using Hash table for new data. (Remove duplicates present in incoming raw data)
    *   - Create a HashSet of the new data.
    *   - Check if it is the first load, then return the deduplicated data and the HashSet. End of story
    * 2 Get ready to merge with old data. -> We know it is not a first load
    *   - Load HashSet of Old Data. If this doesn't exist it should return empty. This might have got deleted.
    *   - Check if existing HashSet is present, rebuild HashSet.
    * 3 Merge
    *   - If no old data is available, this is the fist load, load the new data as is.
    *   - Co group the two data sets. Remove data that was already loaded before, based on available HashSet.
    *   - Now we have the actual data that needs to be appended
    * 4 Create/Update HashSet index
    *   - Create the HashSet Index, for the incoming data set.
    *   - If this is the first run, store this HashSet index.
    *   - If HashSet already existed for old data, update and overwrite the new HashSet.
    *   - Handle failures for writing and storing this HashSet.
    *
    * Returns
    *  - The deduplicated data that should be appended
    *  - The total HashSet index that would be valid after appending the data.
    *  - --- Since the input and output are data frames, hence the HashSet is not overwritten by this etl function.
    *  - --- A helper function is provided to over the HashSet index to disk. However appending Data Frame should
    *  - --- happen first, and then the HashSet should be written. If writing HashSet fails, it should be deleted
    *  - --- to prevent having an invalid HashSet Index. It will get re-created in the next run.
    *
    */

  // TODO Document this method
  // TODO - Return type is the final merged data (to be appended) and the MERGED (Complete) HashSet
  // The entry point for the ETL
  def runEtl(existingData: DataFrame,
             newData: DataFrame,
             hashSetIndexLocation: String,
             partitionColumn: String,
             primaryKeys: Seq[String],
             newDataPartitionEstimator: PartitionEstimator): (DataFrame, RDD[(Int, HashSet[Seq[String]])]) = {

    // 1 Prepare new Data
    // Step 1.1 - Prepare and partition the new data
    val newDataPairRDD = makePairRDD(newData, partitionColumn)

    // Step 1.1 - Partition the new data
    val newDataNumPartitions = newDataPartitionEstimator.estimate
    val newDataPartitioner = new SimpleModuloPartitioner[Int, Row](newDataNumPartitions)
    val newDataPartitionedRDD = newDataPairRDD.partitionBy(newDataPartitioner)

    // Step 1.2 - Remove duplicates from new data.
    val deDupedNewData = deDupPairRDDwithHashSet(newDataPartitionedRDD, primaryKeys = primaryKeys)

    // Step 1.3 - Creat a HashSet Index for the new data
    val newDataHashSet = createHashSetIndex(newDataPartitionedRDD, newDataPartitioner, primaryKeys)


    // Step 1.4 - For first load, return the new data.
    if (isFirstLoad(existingData)) {
      // Existing data is not available, hence HashSet also not available, so no Merge necessary for HashSet Index
      val deDupedNewDataDf = newData
        .sqlContext
        .createDataFrame(deDupedNewData.map(_._2), newData.schema)

      return (deDupedNewDataDf, newDataHashSet)
    }

    // Step 2.1 - Read Or Rebuild old data HashSet
    // It is not the first load now. Regular data load.
    val existingHashSetRDD =
    if (!isFirstLoad(existingData) && !isHashSetIndexAvailable(hashSetIndexLocation)) {
      // Incremental Load but HashSet is not available. Rebuild from scratch for existing data.
      // Rebuilding requires the knowledge of exact number of partitions, and hence is costly.
      val existingPartitions = existingData.select(partitionColumn).distinct.count.toInt // TODO with Spark 1.5.2, the distinct doesn't convert to group by. Add custom strategy
      val existingDataPartitioner = new SimpleModuloPartitioner[Int, Row](existingPartitions)

      val existingPairRDD = makePairRDD(existingData, partitionColumn)
      val existingPartitionedPairRDD = existingPairRDD.partitionBy(existingDataPartitioner)
      createHashSetIndex(existingPartitionedPairRDD, existingDataPartitioner, primaryKeys)
    }
    else
      readHashSetIndex(hashSetIndexLocation).get // We use get, since we know that the HashSet is defined now


    // Step 3 - The actual merge operation
    val filteredRdd = filterOldRecordsUsingHashSet(deDupedNewData, existingHashSetRDD, newDataPartitioner, primaryKeys)


    // Convert the filtered data to a Data Frame
    val mergedDf = newData
      .sqlContext
      .createDataFrame(filteredRdd.map(_._2), newData.schema)

    // Step 4 - Merge the HashSets
    val mergedPartitioner = new SimpleModuloPartitioner[Int, HashSet[Seq[String]]](filteredRdd.partitions.length)
    val mergedHashSet = mergeHashSetIndex(Seq(existingHashSetRDD, newDataHashSet), mergedPartitioner)

    (mergedDf, mergedHashSet)
  }

  private def isHashSetIndexAvailable(hashSetIndexLocation: String): Boolean = {
    val hashSetIndex = readHashSetIndex(hashSetIndexLocation)
    hashSetIndex.isDefined && hashSetIndex.nonEmpty
  }

  private def isFirstLoad(existingData: DataFrame) = existingData.take(1).isEmpty

  private def readHashSetIndex(hashSetIndexLocation: String): Option[RDD[(Int, HashSet[Seq[String]])]] = {
    val filesystemPath = new Path(hashSetIndexLocation)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    if (!fs.exists(filesystemPath)) None
    else {
      val existingHashSetRDD = sparkContext.objectFile[(Int, HashSet[Seq[String]])](hashSetIndexLocation)
      Some(existingHashSetRDD)
    }
  }

  private def deDupPairRDDwithHashSet(dataWithDuplicates: RDD[(Int, Row)],
                                      primaryKeys: Seq[String]): DeDupPairRDD[Int, Row] =
    new DeDupPairRDD[Int, Row](dataWithDuplicates, primaryKeys)

  private def makePairRDD(dataFrame: DataFrame, partitionColumn: String): RDD[(Int, Row)] =
    dataFrame.rdd.keyBy(row => row.getAs[Int](partitionColumn))

  private def createHashSetIndex(partitionedPairRDD: RDD[(Int, Row)],
                                 partitioner: Partitioner,
                                 primaryKeys: Seq[String]): RDD[(Int, HashSet[Seq[String]])] = {
    // Create a HashSet (or a Bloom Filter) index for each partition
    val createCombiner = (row: Row) => HashSet(SerializableHelpers.getKeysFromRow(row.asInstanceOf[GenericRowWithSchema], primaryKeys))
    val mergeValue = (C: HashSet[Seq[String]], V: Row) => C + SerializableHelpers.getKeysFromRow(V.asInstanceOf[GenericRowWithSchema], primaryKeys)
    val mergeCombiners = (C1: HashSet[Seq[String]], C2: HashSet[Seq[String]]) => C1 ++ C2

    // Pass our partitioner to prevent repartitioning / shuffle
    partitionedPairRDD.combineByKey(
      createCombiner = createCombiner,
      mergeValue = mergeValue,
      mergeCombiners = mergeCombiners,
      partitioner = partitioner)
  }


  private def filterOldRecordsUsingHashSet(deDupedNewData: RDD[(Int, Row)],
                                           existingHashSetRDD: RDD[(Int, HashSet[Seq[String]])],
                                           newDataPartitioner: Partitioner,
                                           primaryKeys: Seq[String]): RDD[(Int, Row)] = {
    // Step 3 - Merge - this is an incremental load, and old data is already available.
    // Step 3.1 - Cogroup the data. Passing the partitioner same as new data should prevent repartitioning the data
    val coGroupedRDD = deDupedNewData.cogroup(existingHashSetRDD, partitioner = newDataPartitioner)

    // Step 3.2 - Remove duplicates using old data HashSet - the actual merge operation
    coGroupedRDD.flatMapValues {
      case (vs, Seq()) => // This is a new partition and this wasn't present in old data
        vs.iterator
      case (vs, ws) => // This is a partition which is there in old data as well as new data
        val newRecordsIterator = vs.iterator
        val existingHashSet = ws.iterator.toList.headOption.getOrElse(HashSet()) // We expect only one HashSet
        newRecordsIterator.filter({ newRecord =>
          // Filter already existing data
          !existingHashSet.contains(SerializableHelpers.getKeysFromRow(newRecord.asInstanceOf[GenericRowWithSchema], primaryKeys))
        })
      // Ignore the case for only old partition with no new partition present -> case (Seq(), ws)
    }
  }

  private def mergeHashSetIndex(rdds: Seq[RDD[(Int, HashSet[Seq[String]])]],
                                partitioner: Partitioner): RDD[(Int, HashSet[Seq[String]])] = {
    // Apply reduce to union all the given rdds into one rdd.
    // This would forget the previous partitioning and simply increase the number of partitions.
    val unionedRDD = rdds.reduce(_.union(_))

    // The following function for merging two HashSets would work for merging values as well as Combiners
    val mergeVaulesAndCombiners = (C1: HashSet[Seq[String]], C2: HashSet[Seq[String]]) => C1 ++ C2
    // Because after co grouping we expect only one HashSet

    unionedRDD.combineByKey(createCombiner = (row: HashSet[Seq[String]]) => row,
      mergeValue = mergeVaulesAndCombiners,
      mergeCombiners = mergeVaulesAndCombiners,
      partitioner = partitioner) // This is required to force repartitioning as union would have likely increased the partitions
  }

  def overwriteHashSetRDD(hashSetIndexLocation: String, rdd: RDD[(Int, HashSet[Seq[String]])]) = {
    // TODO Should it write to a tmp folder underneath and on success move the files ???

    // Delete already existing file
    val filesystemPath = new Path(hashSetIndexLocation)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    if (fs.exists(filesystemPath))
      fs.delete(filesystemPath, true)

    // Write to the specified location
    Try(rdd.saveAsObjectFile(hashSetIndexLocation))
    match {
      case Failure(ex) => println("FATAL ERROR : Failed to save index. Index location will be deleted.")
        if (fs.exists(filesystemPath))
          fs.delete(filesystemPath, true)
      case Success(_) =>
    }
  }
}

