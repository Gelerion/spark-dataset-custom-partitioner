package org.apache.spark.sql.exchange.examples

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner

import scala.util.Random

object RepartitionPersonsByNameDataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkBuilder.getSpark()

    import org.apache.spark.sql.exchange.implicits._
    import spark.implicits._

    val persons: Dataset[Person] = spark.createDataset(makePersons())
    println("Num partitions BEFORE repartitioning: " + persons.rdd.getNumPartitions)

    val partitioned = persons.repartitionBy(new PersonByFirstCharPartitioner)

    println("Num partitions AFTER repartitioning: " + partitioned.rdd.getNumPartitions)
    partitioned.rdd.glom().collect().zipWithIndex.foreach {
      item: (Array[Person], Int) => println(s"partition #${item._2}: ${item._1.take(10).mkString(",")}")
    }
  }

  def makePersons(): Seq[Person] = {
    val names = Array("Max","Eitan", "Dan", "Viktor","Denis","Marat", "Rebys")
    val size = names.length

    for(i <- 0 to 50000) yield {
      Person(names(i % size), i / 1000, Random.nextInt(30000).toString)
    }
  }

}

class PersonByFirstCharPartitioner extends TypedPartitioner[Person] {
  override def getPartitionIdx(person: Person): Int = if (person.name.startsWith("D")) 0 else 1
  override def numPartitions: Int = 2
}

case class Person(name: String, age: Int, id: String)
