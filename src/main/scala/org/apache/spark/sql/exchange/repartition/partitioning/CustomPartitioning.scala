package org.apache.spark.sql.exchange.repartition.partitioning

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution, Partitioning}
import org.apache.spark.sql.exchange.repartition.partitioner.CustomPartitioner
import org.apache.spark.sql.types.DataType

trait CustomPartitioning extends Partitioning {

  val partitioner: CustomPartitioner

  def getPartitionKey: InternalRow => Any

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case hashDistribution: HashClusteredDistribution
          if partitioner.partitionKeys.isDefined && hashDistribution.expressions.forall(expr => expr.isInstanceOf[AttributeReference]) =>

          val thatPartitionKeys: Set[(String, DataType)] = hashDistribution
            .expressions
            .map(_.asInstanceOf[AttributeReference])
            .map(partKey => (partKey.name, partKey.dataType)).toSet

          thatPartitionKeys == this.partitioner.partitionKeys.get

        case ClusteredDistribution(expressions, requiredNumPartitions)
          if partitioner.partitionKeys.isDefined && expressions.forall(expr => expr.isInstanceOf[AttributeReference]) =>

          val thatPartitionKeys: Set[(String, DataType)] = expressions
            .map(_.asInstanceOf[AttributeReference])
            .map(partKey => (partKey.name, partKey.dataType)).toSet

          thatPartitionKeys == this.partitioner.partitionKeys.get

          //val if(requiredNumPartitions.isDefined) requiredNumPartitions.get == this.partitioner.numPartitions : true
        case _ => false
      }
    }
  }

//  override def toString: String = s"${this.getClass.getSimpleName}"
  override def toString: String =
    if (s"${partitioner.getClass.getSimpleName}".contains("anon"))
      s"${this.getClass.getSimpleName}"
    else
      s"${partitioner.getClass.getSimpleName}"

}
