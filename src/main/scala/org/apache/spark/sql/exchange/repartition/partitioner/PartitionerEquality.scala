package org.apache.spark.sql.exchange.repartition.partitioner

import org.apache.spark.Partitioner

trait PartitionerEquality {
  self: Partitioner =>

  /**
   * Two different instances of partitioner are considered equals when:
   * 1. They are equals by identity
   * 2. They have the same number of partitions
   * Or
   * 1. They share the same group id
   */
  def groupId: Option[Int] = None

  override def equals(obj: Any): Boolean = obj match {
    case that: PartitionerEquality =>
      areEquals(that) && this.getNumPartitions == that.getNumPartitions
    case _ => false
  }

  private def areEquals(that: PartitionerEquality): Boolean = {
    groupId match {
      case Some(id) if that.groupId.isDefined => id == that.groupId.get
        //check by instance equality
      case None => that.canEqual(this) && that.eq(this)
    }
  }

  private def canEqual(a: Any): Boolean = a.isInstanceOf[self.type]

  private def getNumPartitions: Int = self.numPartitions

}
