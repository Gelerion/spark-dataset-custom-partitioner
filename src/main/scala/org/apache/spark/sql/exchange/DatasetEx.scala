package org.apache.spark.sql.exchange

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.exchange.logical.RepartitionByCustom
import org.apache.spark.sql.exchange.repartition.partitioner.{RowPartitioner, TypedPartitioner}
import org.apache.spark.sql.exchange.repartition.partitioning.{InternalRowPartitioning, InternalTypedPartitioning}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import scala.reflect.ClassTag

object DatasetEx

object implicits {
  @inline private def withTypedPlan[U: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }

  @inline private def withPlan(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  implicit class DatasetEx[T <: Product : ClassTag : Encoder](private val underlying: Dataset[T]) {
    private[sql] lazy val exprEnc: ExpressionEncoder[T] = encoderFor[T]
    private[sql] lazy val deserializer: Expression =
      exprEnc.resolveAndBind(underlying.logicalPlan.output, underlying.sparkSession.sessionState.analyzer).deserializer

    def repartitionBy(partitioner: TypedPartitioner[T]): Dataset[T] = {
      val partitioning = new InternalTypedPartitioning(partitioner, deserializer)
      withTypedPlan(underlying.sparkSession, RepartitionByCustom(underlying.logicalPlan, partitioning))
    }
  }

  implicit class DataFrameEx(private val underlying: DataFrame) {

    def repartitionBy(partitioner: RowPartitioner): DataFrame = {
      val partitioning = new InternalRowPartitioning(partitioner, makeDeserializer)
      withPlan(underlying.sparkSession, RepartitionByCustom(underlying.logicalPlan, partitioning))
    }

    private def makeDeserializer: Expression =  {
      val logicalPlan = underlying.logicalPlan
      val analyzer = underlying.sparkSession.sessionState.analyzer

      val qe = underlying.sparkSession.sessionState.executePlan(logicalPlan)
      qe.assertAnalyzed()
      RowEncoder(qe.analyzed.schema).resolveAndBind(logicalPlan.output, analyzer).deserializer
    }
  }
}