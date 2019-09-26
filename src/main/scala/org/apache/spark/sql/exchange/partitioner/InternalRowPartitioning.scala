package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection

private [exchange] class InternalRowPartitioning(val partitioner: RowPartitioner, val deserializer: Expression)
  extends CustomPartitioning with Serializable {

  def getPartitionKey: InternalRow => Row = {
    row => {
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      objProj(row).get(0, null).asInstanceOf[Row]
    }
  }

  override val numPartitions: Int = partitioner.numPartitions
}
