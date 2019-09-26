package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.sql.Row

abstract class RowPartitioner extends Partitioner with Serializable {

  override def getPartition(key: Any): Int = getPartitionIdx(key.asInstanceOf[Row])

  def getPartitionIdx(row: Row): Int

  def numPartitions: Int

  override def equals(obj: Any): Boolean = obj match {
    case that: RowPartitioner =>
      that.canEqual(this) && that.eq(this) && that.numPartitions == this.numPartitions
    case _ => false
  }

  private def canEqual(a: Any) = a.isInstanceOf[RowPartitioner]
}
