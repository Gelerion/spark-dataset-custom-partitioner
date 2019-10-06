package org.apache.spark.sql.exchange.repartition.partitioning

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.exchange.repartition.partitioner.RowPartitioner

private [exchange] class InternalRowPartitioning(val partitioner: RowPartitioner, val deserializer: Expression)
  extends CustomPartitioning with Serializable {

  def getPartitionKey: InternalRow => Row = {
    row => {
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      objProj(row).get(0, null).asInstanceOf[Row]
    }
  }

  override val numPartitions: Int = partitioner.numPartitions

  override def equals(obj: Any): Boolean = obj match {
    case that: InternalRowPartitioning => that.canEqual(this) && this.partitioner == that.partitioner
    case _ => false
  }

  private def canEqual(a: Any): Boolean = a.isInstanceOf[InternalRowPartitioning]
}
