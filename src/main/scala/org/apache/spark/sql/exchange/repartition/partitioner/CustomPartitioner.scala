package org.apache.spark.sql.exchange.repartition.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.sql.types.DataType

trait CustomPartitioner extends Partitioner {
  type PartitionKey = (String, DataType)

  def partitionKeys: Option[Set[PartitionKey]] = None
}
