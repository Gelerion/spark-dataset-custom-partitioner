package org.apache.spark.sql.exchange.integration

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.spark.Partitioner
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.exchange.partitioner.TypedPartitioner
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.scalatest.BeforeAndAfterEach

class JoinDatasetWithCustomPartitioner extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
  }

  import org.apache.spark.sql.exchange.implicits._
  import testImplicits._

  //broadcast join -> ReuseExchange
  test("The same partitioner, join") {
    val depPartitioner = new DepartmentByIdPartitioner

    val dep_1 = department.repartitionBy(depPartitioner)
    val dep_2 = department.repartitionBy(depPartitioner)

    val joined = dep_1.join(dep_2, Seq("id"))
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
  }

  test("The same partitioner, anonymous class, join") {
    val depPartitioner = new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    }

    val dep_1 = department.repartitionBy(depPartitioner)
    val dep_2 = department.repartitionBy(depPartitioner)

    val joined = dep_1.join(dep_2, Seq("id"))
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
  }

  test("Different instances, with the same logic") {
    val dep_1 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    })

    val dep_2 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    })

//    val joined = dep_1.join(dep_2, Seq("id"))
//    joined.explain(true)
//    joined.show()
  }

  //sort join -> satisfies distribution
  //hash join -> satisfies distribution

  lazy val department: Dataset[Dept] =
    Seq(
      Dept("a", "ant dept"),
      Dept("d", "duck dept"),
      Dept("c", "cat dept"),
      Dept("r", "rabbit dept"),
      Dept("b", "badger dept"),
      Dept("z", "zebra dept"),
      Dept("m", "mouse dept")
    ).toDS()

  lazy val employers: Dataset[Emp] =
    Seq(
      Emp("emp val 1", "a"),
      Emp("emp val 2", "d"),
      Emp("emp val 3", "c"),
      Emp("emp val 4", "r"),
      Emp("emp val 5", "b"),
      Emp("emp val 6", "z"),
      Emp("emp val 7", "m")
    ).toDS()
}