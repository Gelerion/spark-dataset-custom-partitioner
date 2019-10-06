package org.apache.spark.sql.exchange.integration.joins

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.exchange.integration.{DepartmentByIdPartitioner, Dept, Emp}
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.scalatest.BeforeAndAfterEach

class SortMergeJoinWithCustomPartitioner extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
  }

  import org.apache.spark.sql.exchange.implicits._
  import testImplicits._
  //sort join -> satisfies distribution

  test("The same partitioner, join") {
    val depPartitioner = new DepartmentByIdPartitioner

    val dep_1 = department.repartitionBy(depPartitioner)
    val dep_2 = department.repartitionBy(depPartitioner)

    val joined = dep_1.join(dep_2, Seq("id"))

    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
    //verify SMJ
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined)

    checkData(joined)
  }

  test("The same partitioner, anonymous class, join") {
    val depPartitioner = new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("id", StringType)))
    }

    val dep_1 = department.repartitionBy(depPartitioner)
    val dep_2 = department.repartitionBy(depPartitioner)

    val joined = dep_1.join(dep_2, Seq("id"))
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
    //verify SMJ
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined)

    checkData(joined)
  }

  test("Different partitioner instances, with the same group id, join") {
    val dep_1 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("id", StringType)))
    })

    val dep_2 = department.repartitionBy(new TypedPartitioner[Dept] {
      override def getPartitionIdx(value: Dept): Int = if (value.id.startsWith("a")) 0 else 1
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("id", StringType)))
    })

    val joined = dep_1.join(dep_2, Seq("id"))
    joined.explain(true)
    joined.show()
    //should re-use exchange
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[ReusedExchangeExec]).isDefined)
    //verify SMJ
    assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[SortMergeJoinExec]).isDefined)

    checkData(joined)
  }

  private def checkData(df: DataFrame): Unit = {
    val result = df.rdd.glom().collect().zipWithIndex.map(_.swap)
    assert(df.rdd.getNumPartitions == 2)
    assert(result.length == 2)
    assert(result(0)._2.length == 1) //starts with 'a' partition
    assert(result(1)._2.length == 6) //rest
  }

  override protected def sparkConf: SparkConf = {
    Logger.getLogger("org").setLevel(Level.OFF)
    new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "0")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
  }

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
