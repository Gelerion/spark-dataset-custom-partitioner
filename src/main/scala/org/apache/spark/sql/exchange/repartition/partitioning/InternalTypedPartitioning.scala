package org.apache.spark.sql.exchange.repartition.partitioning

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner

private [exchange] class InternalTypedPartitioning[T](val partitioner: TypedPartitioner[T], deserializer: Expression)
  extends CustomPartitioning with Serializable {

  def getPartitionKey: InternalRow => T = {
    row => {
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }

  override val numPartitions: Int = partitioner.numPartitions

  override def equals(obj: Any): Boolean = obj match {
    case that: InternalTypedPartitioning[_] => that.canEqual(this) && this.partitioner == that.partitioner
    case _ => false
  }

  private def canEqual(a: Any): Boolean = a.isInstanceOf[InternalTypedPartitioning[_]]
}

