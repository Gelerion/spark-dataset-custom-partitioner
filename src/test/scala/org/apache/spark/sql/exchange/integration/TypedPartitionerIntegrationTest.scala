package org.apache.spark.sql.exchange.integration

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.types.{DataType, StringType}
import org.scalatest.BeforeAndAfterEach

class TypedPartitionerIntegrationTest extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
  }

  import org.apache.spark.sql.exchange.implicits._
  import testImplicits._

  test("should properly repartition by user defined partitioner") {
    val customPartitioner = new DepartmentByIdPartitioner

    val repartitioned = department.repartitionBy(customPartitioner)

    val result = repartitioned.rdd.glom().collect().zipWithIndex.map(_.swap)

    assert(repartitioned.rdd.getNumPartitions == 2)
    assert(result.length == 2)
    //starts with 'a' partition
    assert(result(0)._2.length == 1)
    //rest
    assert(result(1)._2.length == 6)
  }

  test("should repartition with Pairs") {
    val repartitioned = departmentPair.repartitionBy(new TypedPartitioner[(String, String, Int)] {
      override def getPartitionIdx(value: (String, String, Int)): Int = if (value._1.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    })

    val result = repartitioned.rdd.glom().collect().zipWithIndex.map(_.swap)

    assert(repartitioned.rdd.getNumPartitions == 2)
    assert(result.length == 2)
    //starts with 'a' partition
    assert(result(0)._2.length == 1)
    //rest
    assert(result(1)._2.length == 6)
  }

  lazy val department: Dataset[Dept] = spark.sparkContext.parallelize(
    Seq(
      Dept("a", "ant dept"),
      Dept("d", "duck dept"),
      Dept("c", "cat dept"),
      Dept("r", "rabbit dept"),
      Dept("b", "badger dept"),
      Dept("z", "zebra dept"),
      Dept("m", "mouse dept")
    )
  ).toDS()

  lazy val departmentPair: Dataset[(String, String, Int)] = spark.sparkContext.parallelize(
    Seq(
      ("a", "ant dept", 1),
      ("d", "duck dept", 1),
      ("c", "cat dept", 1),
      ("r", "rabbit dept", 6),
      ("b", "badger dept", 5),
      ("z", "zebra dept", 4),
      ("m", "mouse dept", 2)
    )
  ).toDS()
}

class DepartmentByIdPartitioner extends TypedPartitioner[Dept] {
  override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
  override def numPartitions: Int = 2

  override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("id", StringType)))
}
