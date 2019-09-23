package com.gelerion.spark.dataset.partitioner

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.exchange.execution.ShuffleExchangeWithCustomPartitionerExec
import org.apache.spark.sql.exchange.logical.RepartitionByCustom
import org.apache.spark.sql.execution.SparkPlan

object RepartitionByCustomStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case repartitionByCustom: RepartitionByCustom =>
      ShuffleExchangeWithCustomPartitionerExec(repartitionByCustom.partitioning, planLater(repartitionByCustom.child)) :: Nil
    // plan could not be applied
    case _ => Nil
  }

}
