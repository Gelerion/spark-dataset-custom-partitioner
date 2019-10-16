package org.apache.spark.sql.exchange.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.exchange.integration.Dept
import org.apache.spark.sql.exchange.repartition.partitioner.RowPartitioner

object RepartitionRowsByKeyDataframe {

  def main(args: Array[String]): Unit = {
    val spark = SparkBuilder.getSpark()

    import org.apache.spark.sql.exchange.implicits._
    import spark.implicits._

    val departments = Seq(
      Dept("a", "ant dept"),
      Dept("d", "duck dept"),
      Dept("c", "cat dept"),
      Dept("r", "rabbit dept"),
      Dept("b", "badger dept"),
      Dept("z", "zebra dept"),
      Dept("m", "mouse dept")
    ).toDF("id", "value")

    println("Num partitions BEFORE repartitioning: " + departments.rdd.getNumPartitions)
    val partitioned = departments.repartitionBy(new RowPartitioner {
      override def getPartitionIdx(row: Row): Int =
        if (row.getAs[String]("id").startsWith("d")) 0 else 1
      override def numPartitions: Int = 2
    })

    println("Num partitions AFTER repartitioning: " + partitioned.rdd.getNumPartitions)
    partitioned.rdd.glom().collect().zipWithIndex.foreach {
      item: (Array[Row], Int) => println(s"partition #${item._2}: ${item._1.take(10).mkString(",")}")
    }
  }
}
