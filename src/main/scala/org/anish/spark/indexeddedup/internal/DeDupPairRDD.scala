package org.anish.spark.indexeddedup.internal

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable.{HashSet => MHashSet}

/**
  * Extension of RDD. This creates a de duplicated RDD for each partition based on HashSet
  *
  * Created by anish on 15/04/17.
  */
private[indexeddedup] class DeDupPairRDD[K, V <: Row](@transient var prev: RDD[_ <: Product2[K, V]],
                                                      // Column names based on which it will dedup
                                                      // (primary keys in a partition)
                                                      cols: Seq[String])
  extends RDD[(K, V)](prev) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iterList = firstParent[(K, V)].iterator(split, context).toList

    if (iterList.nonEmpty) {
      val hashSetForPartition: MHashSet[Seq[Any]] = MHashSet()

      iterList.filter(record => {
        // Cast to GenericRowWithSchema to enable access by field name
        val keysInRecord = getKeysFromRow(record._2.asInstanceOf[GenericRowWithSchema])
        if (hashSetForPartition.contains(keysInRecord)) {
          false
        }
        else {
          hashSetForPartition.+=(keysInRecord)
          true
        }
      }).toIterator
    }
    else firstParent[(K, V)].iterator(split, context) // The partition is empty
  }

  // Keeps partitions intact. It is considered that the RDD is already partitioned optimally.
  // Also it will have one to one depdency
  override protected def getPartitions: Array[Partition] = firstParent[(K, V)].partitions

  private def getKeysFromRow(row: GenericRowWithSchema): Seq[Any] = cols.map(fieldName => row.getAs[Any](fieldName))
}
