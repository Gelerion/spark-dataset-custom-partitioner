package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

trait CustomPartitioning extends Partitioning {

  val partitioner: Partitioner

  def getPartitionKey: InternalRow => Any

//  override def toString: String = s"${this.getClass.getSimpleName}"
  override def toString: String =
    if (s"${partitioner.getClass.getSimpleName}".contains("anon"))
      s"${this.getClass.getSimpleName}"
    else
      s"${partitioner.getClass.getSimpleName}"

}
