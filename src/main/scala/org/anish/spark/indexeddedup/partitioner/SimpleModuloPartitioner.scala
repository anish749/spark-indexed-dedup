package org.anish.spark.indexeddedup.partitioner

import org.apache.spark.Partitioner

import scala.reflect.ClassTag

/**
  * A Partitioner which puts different values into different buckets. Doesn't use Object.hashCode to determine the
  * partition. It will always partition the data into exactly the number of partitions specified. Partitions can be skewed.
  *
  * Created by anish on 22/04/17.
  */
class SimpleModuloPartitioner[K: Ordering : ClassTag, V](partitions: Int)
  extends Partitioner {

  /** Calculates 'x' modulo 'mod', takes to consideration sign of x,
    * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
    * so function return (x % mod) + mod in that case.
    *
    * This function is taken from Spark's HashPartitioner
    */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case key: Int => nonNegativeMod(key.asInstanceOf[Int], numPartitions)
    case key: Double => nonNegativeMod(key.asInstanceOf[Double].toInt, numPartitions)
    // Long is not defined as numPartitions should be Int as per Spark Partitioner class
    case _ =>
      throw new RuntimeException("Key is not Int or Double or it is not supported yet")
  }

  override def equals(other: Any): Boolean = other match {
    case s: SimpleModuloPartitioner[K, V] =>
      s.numPartitions == numPartitions
    case _ =>
      false
  }

}