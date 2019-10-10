package org.apache.spark.sql.exchange.integration.groupings

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, ConvertToLocalRelation}
import org.apache.spark.sql.exchange.integration.{DeptDetails, JoinDept}
import org.apache.spark.sql.exchange.repartition.partitioner.{RowPartitioner, TypedPartitioner}
import org.apache.spark.sql.exchange.test.{SharedSparkSession, SparkFunSuite}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.BeforeAndAfterEach

class GroupByWithCustomPartitioner extends SparkFunSuite with SharedSparkSession with BeforeAndAfterEach {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
  }

  import org.apache.spark.sql.exchange.implicits._
  import testImplicits._

  test("Group by, row partitioner") {
    val repartitioned = data.repartitionBy(new RowPartitioner {
      override def getPartitionIdx(row: Row): Int =
        if (row.getAs[String]("department") == "dep1") 0 else 1
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("department", StringType)))
    })

    assert(repartitioned.rdd.getNumPartitions == 2)

    val grouped = repartitioned.groupBy($"department")
      .agg(sum($"salary").as("sum"))

    assert(grouped.rdd.getNumPartitions == 2)

    //doesn't have an additional shuffle step
    assert(grouped.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)

    val result = grouped.rdd.glom().collect()
    //first partition, first row
    assert(result(0)(0).getAs[Long]("sum") == 2200)

    //second partition, first row
    assert(result(1)(0).getAs[Long]("sum") == 3300)
    //second partition, second row
    assert(result(1)(1).getAs[Long]("sum") == 1000)
  }

  test("Join then group by, row partitioner") {
    val customPartitioner = new RowPartitioner {
      override def getPartitionIdx(row: Row): Int =
        if (row.getAs[String]("department") == "dep1") 0 else 1
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("department", StringType)))
    }

    val data_1 = left.repartitionBy(customPartitioner)
    val data_2 = right.repartitionBy(customPartitioner)

    val joined = data_1.join(data_2, Seq("department"))
    assert(joined.rdd.getNumPartitions == 2)

    val grouped = joined.groupBy($"department")
      .agg(sum($"salary").as("sum"))

    assert(grouped.rdd.getNumPartitions == 2)
    //doesn't have an additional shuffle step
    assert(grouped.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)

    val result = grouped.rdd.glom().collect()
    //first partition, first row
    assert(result(0)(0).getAs[Long]("sum") == 2200)

    //second partition, first row
    assert(result(1)(0).getAs[Long]("sum") == 3300)
    //second partition, second row
    assert(result(1)(1).getAs[Long]("sum") == 1000)
  }

  test("Join dataset then group by, row partitioner") {
    val deptPartitioner = new TypedPartitioner[JoinDept] {
      override def getPartitionIdx(value: JoinDept): Int =
        if(value.department.equals("dep1")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("department", StringType)))
      override def toString: String = "TypedPartitioner[JoinDept]"
    }

    val deptDetailsPartitioner = new TypedPartitioner[DeptDetails] {
      override def getPartitionIdx(value: DeptDetails): Int =
        if(value.department.equals("dep1")) 0 else 1
      override def numPartitions: Int = 2
      override def groupId: Option[Int] = Some(1)
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("department", StringType)))
      override def toString: String = "TypedPartitioner[DeptDetails]"
    }

    val data_1 = leftDS.repartitionBy(deptPartitioner)
    val data_2 = rightDS.repartitionBy(deptDetailsPartitioner)

    val joined = data_1.join(data_2, Seq("department"))
    assert(joined.rdd.getNumPartitions == 2)

    val grouped = joined.groupBy($"department")
      .agg(sum($"salary").as("sum"))

    assert(grouped.rdd.getNumPartitions == 2)
    //doesn't have an additional shuffle step
    assert(grouped.queryExecution.executedPlan.find(_.isInstanceOf[ShuffleExchangeExec]).isEmpty)

    val result = grouped.rdd.glom().collect()
    //first partition, first row
    assert(result(0)(0).getAs[Double]("sum") == 2200)

    //second partition, first row
    assert(result(1)(0).getAs[Double]("sum") == 3300)
    //second partition, second row
    assert(result(1)(1).getAs[Double]("sum") == 1000)
  }

  override protected def sparkConf: SparkConf = {
    Logger.getLogger("org").setLevel(Level.OFF)
    new SparkConf()
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
//      .set("spark.sql.autoBroadcastJoinThreshold", "0")
      .set("spark.network.timeout", "10000001")
      .set("spark.executor.heartbeatInterval", "10000000")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
//      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, PushDownPredicate.ruleName)
//      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ReorderJoin.ruleName)
//      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, PushPredicateThroughJoin.ruleName)
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ColumnPruning.ruleName)
  }

  lazy val data: DataFrame = Seq(
    ("dep1", "M", 1200, 34),
    ("dep1", "M", 800, 30),
    ("dep1", "F", 200, 21),
    ("dep2", "M", 1000, 21),
    ("dep2", "M", 1200, 22),
    ("dep2", "F", 500, 24),
    ("dep2", "M", 600, 44),
    ("dep3", "F", 100, 44),
    ("dep3", "M", 300, 44),
    ("dep3", "M", 600, 44)
  ).toDF("department", "gender", "salary", "age")

  lazy val left: DataFrame = Seq(
    ("dep1", "M"),
    ("dep2", "M"),
    ("dep3", "M")
  ).toDF("department", "gender")

  lazy val right: DataFrame = Seq(
    ("dep1", 1200, 34),
    ("dep1", 800, 30),
    ("dep1", 200, 21),
    ("dep2", 1000, 21),
    ("dep2", 1200, 22),
    ("dep2", 500, 24),
    ("dep2", 600, 44),
    ("dep3", 100, 44),
    ("dep3", 300, 44),
    ("dep3", 600, 44)
  ).toDF("department", "salary", "age")

  lazy val leftDS: Dataset[JoinDept] = Seq(
    JoinDept("dep1", "M"),
    JoinDept("dep2", "M"),
    JoinDept("dep3", "M")
  ).toDS()

  lazy val rightDS: Dataset[DeptDetails] = Seq(
    DeptDetails("dep1", "1200", 34),
    DeptDetails("dep1", "800", 30),
    DeptDetails("dep1", "200", 21),
    DeptDetails("dep2", "1000", 21),
    DeptDetails("dep2", "1200", 22),
    DeptDetails("dep2", "500", 24),
    DeptDetails("dep2", "600", 44),
    DeptDetails("dep3", "100", 44),
    DeptDetails ("dep3", "300", 44),
    DeptDetails("dep3", "600", 44)
  ).toDS()
}


