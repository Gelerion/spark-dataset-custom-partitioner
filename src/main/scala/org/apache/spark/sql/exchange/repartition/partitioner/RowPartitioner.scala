package org.apache.spark.sql.exchange.repartition.partitioner

import org.apache.spark.sql.Row

abstract class RowPartitioner extends CustomPartitioner with PartitionerEquality with Serializable {
  override def getPartition(key: Any): Int = getPartitionIdx(key.asInstanceOf[Row])

  def getPartitionIdx(row: Row): Int

  def numPartitions: Int
}
