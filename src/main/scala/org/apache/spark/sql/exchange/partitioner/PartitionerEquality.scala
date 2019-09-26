package org.apache.spark.sql.exchange.partitioner

trait PartitionerEquality {

  def isEligiblePartitioner: Boolean
}
