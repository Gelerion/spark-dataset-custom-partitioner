package org.apache.spark.sql.exchange.integration.joins

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.spark.sql.exchange.integration.{DepartmentByIdPartitioner, Dept, Emp}
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.BeforeAndAfterEach

class BroadcastJoinWithCustomPartitioner extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach {

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
    checkData(joined)
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

    checkData(joined)
  }

  test("Different partitioner instances, with the same group id, join") {
    //we explicitly define the same group id so that Spark knows tat these two are the same
    val dep_1 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
    })

    val dep_2 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
    })

    val joined = dep_1.join(dep_2, Seq("id"))
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)

    checkData(joined)
  }

  test("Different partitioner instances, different ds types, none group id, join") {
    //we explicitly define the same group id so that Spark knows tat these two are the same
    val dep_1 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    })

    val dep_2 = employers.repartitionBy(new TypedPartitioner[Emp] {
      override def getPartitionIdx(value: Emp): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
    })

    val joined = dep_1.join(dep_2, Seq("id"))
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isEmpty)

    checkData(joined)
  }

  test("Different partitioner instances, different ds types, the same group id, join") {
    //we explicitly define the same group id so that Spark knows that these two are the same
    val dep_1 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
    })

    val dep_2 = employers.repartitionBy(new TypedPartitioner[Emp] {
      override def getPartitionIdx(value: Emp): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
    })

    //It has two main issues
    // 1. Different order of fields leads to different keys in Exchange
    //    {val sameSchema = exchanges.getOrElseUpdate(exchange.schema, ArrayBuffer[Exchange]())}
    // 2. Spark checks input record for local scan and files for file scan to ensure the same source is used
    //Hence ReusedExchangeExec can;t be build
    val joined = dep_1.join(dep_2, Seq("id"))

    //assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isEmpty)

    checkData(joined)
  }


/*  test("Dataframe, the same partitioner, join") {
    val customPartitioner = new FirstCharRowPartitioner

    val dep_1 = departmentDf.repartitionBy(customPartitioner)
    val dep_2 = departmentDf.repartitionBy(customPartitioner)

//    val dep_1 = departmentDf.repartition(2, $"id")
//    val dep_2 = departmentDf.repartition(2, $"id")

    val joined = dep_1.join(dep_2, Seq("id"))
//    joined.explain(true)
//    joined.show()
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
  }*/


  private def checkData(df: DataFrame): Unit = {
    val result = df.rdd.glom().collect().zipWithIndex.map(_.swap)
    assert(df.rdd.getNumPartitions == 2)
    assert(result.length == 2)
    assert(result(0)._2.length == 1) //starts with 'a' partition
    assert(result(1)._2.length == 6) //rest
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
      Emp("emp val a", "a"),
      Emp("emp val d", "d"),
      Emp("emp val c", "c"),
      Emp("emp val r", "r"),
      Emp("emp val b", "b"),
      Emp("emp val z", "z"),
      Emp("emp val m", "m")
    ).toDS()

  lazy val departmentDf: DataFrame = Seq(
    Dept("a", "ant dept"),
    Dept("d", "duck dept"),
    Dept("c", "cat dept"),
    Dept("r", "rabbit dept"),
    Dept("b", "badger dept"),
    Dept("z", "zebra dept"),
    Dept("m", "mouse dept")
  ).toDF("id", "value")
}