package org.apache.spark.sql.exchange.integration

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.spark.sql.exchange.partitioner.RowPartitioner
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach

class RowPartitionerIntegrationTest extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach  {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
  }

  import org.apache.spark.sql.exchange.implicits._
  import testImplicits._

  test("should properly repartition by user defined partitioner") {
    val customPartitioner = new FirstCharRowPartitioner

    val repartitioned = department.repartitionBy(customPartitioner)

    val result = repartitioned.rdd.glom().collect().zipWithIndex.map(_.swap)

    assert(repartitioned.rdd.getNumPartitions == 2)
    assert(result.length == 2)
    //starts with 'a' partition
    assert(result(0)._2.length == 1)
    //rest
    assert(result(1)._2.length == 6)
  }

  lazy val department: DataFrame = Seq(
      Dept("a", "ant dept"),
      Dept("d", "duck dept"),
      Dept("c", "cat dept"),
      Dept("r", "rabbit dept"),
      Dept("b", "badger dept"),
      Dept("z", "zebra dept"),
      Dept("m", "mouse dept")
    ).toDF("id", "value")
}


class FirstCharRowPartitioner extends RowPartitioner {

  override def getPartitionIdx(row: Row): Int =
    if (row.getAs[String]("id").startsWith("a")) 0 else 1

  override def numPartitions: Int = 2
}