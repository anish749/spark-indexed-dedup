package org.anish.spark.indexeddedup.partitioner

import org.anish.spark.SimpleSparkSession
import org.apache.spark.Partitioner
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by anish on 25/04/17.
  */
class SimpleModuloPartitionerTest extends FlatSpec with Matchers {

  behavior of "Simple Modulo Partitioner"
  it should "distribute different values into different partitions (Key is of type Int)" in {
    val simpleModuloPartitioner: Partitioner = new SimpleModuloPartitioner[Int, String](3)

    val sc = SimpleSparkSession.sparkContext

    val pairRDD = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three"), (3, "threeAgain")))
    val partitionedRDD = pairRDD.partitionBy(simpleModuloPartitioner)

    // Verify counts so that all partitions have been accessed.
    partitionedRDD.mapPartitions(iter => {
      val list = iter.toList
      val partitionValue = list.headOption.getOrElse((-1, None))._1
      Iterator((partitionValue, list.length))
    }).collect.toList shouldBe List((3, 2), (1, 1), (2, 1))

    simpleModuloPartitioner.getPartition(1) shouldBe 1
    simpleModuloPartitioner.getPartition(2) shouldBe 2
    simpleModuloPartitioner.getPartition(3) shouldBe 0
  }

  it should "distribute different values into different partitions (Key is of type Double)" in {
    val simpleModuloPartitioner: Partitioner = new SimpleModuloPartitioner[Double, String](3)

    val sc = SimpleSparkSession.sparkContext

    val pairRDD = sc.parallelize(Seq((1.0, "one"), (2.0, "two"), (3.0, "three"), (3.0, "threeAgain")))
    val partitionedRDD = pairRDD.partitionBy(simpleModuloPartitioner)

    // Verify counts so that all partitions have been accessed.
    partitionedRDD.mapPartitions(iter => {
      val list = iter.toList
      val partitionValue = list.headOption.getOrElse((-1, None))._1
      Iterator((partitionValue, list.length))
    }).collect.toList shouldBe List((3, 2), (1, 1), (2, 1))

    simpleModuloPartitioner.getPartition(1) shouldBe 1
    simpleModuloPartitioner.getPartition(2) shouldBe 2
    simpleModuloPartitioner.getPartition(3) shouldBe 0
  }

}
