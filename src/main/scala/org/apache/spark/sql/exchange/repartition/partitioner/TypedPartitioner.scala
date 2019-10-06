package org.apache.spark.sql.exchange.repartition.partitioner

/**
 * Must not be inner class
 */
abstract class TypedPartitioner[T] extends CustomPartitioner with PartitionerEquality with Serializable {
  override def getPartition(key: Any): Int = getPartitionIdx(key.asInstanceOf[T])

  def getPartitionIdx(value: T): Int

  def numPartitions: Int
}