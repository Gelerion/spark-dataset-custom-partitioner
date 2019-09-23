package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.Partitioner

/**
 * Must not be inner class
 */
abstract class TypedPartitioner[T] extends Partitioner with Serializable {

  override def getPartition(key: Any): Int = getPartitionIdx(key.asInstanceOf[T])

  def getPartitionIdx(value: T): Int

  def numPartitions: Int
}