package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.Partitioner

/**
 * Must not be inner class
 */
abstract class TypedPartitioner[T] extends Partitioner with Serializable {

  override def getPartition(key: Any): Int = getPartitionIdx(key.asInstanceOf[T])

  def getPartitionIdx(value: T): Int

  def numPartitions: Int

  override def equals(obj: Any): Boolean = obj match {
    case that: TypedPartitioner[_] =>
      that.canEqual(this) && that.eq(this) && that.numPartitions == this.numPartitions
    case _ => false
  }

  private def canEqual(a: Any): Boolean = a.isInstanceOf[TypedPartitioner[T]]
}