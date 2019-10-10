package org.apache.spark.sql.exchange.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.exchange.repartition.partitioning.CustomPartitioning
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan, UnsafeRowSerializer}
import org.apache.spark.util.MutablePair
import org.apache.spark.{Partitioner, ShuffleDependency, SparkEnv}

case class ShuffleExchangeWithCustomPartitionerExec(partitioning: Partitioning, child: SparkPlan)
  extends Exchange {

  //1. register metrics
  override lazy val metrics: Map[String, SQLMetric] =
    Map("dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  //2. nodeName
  override def nodeName: String = {
    s"Exchange (custom partitioner)"
  }

  //3. outputPartitioning
  /** Specifies how data is partitioned across different nodes in the cluster. */
  override def outputPartitioning: Partitioning = partitioning

  //4. doPrepare()
  /*
  There is no sense in defining adaptive query executor (aka coordinator)
  as custom partitioner aims to distribute the data in the best way
   */

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val shuffleDependency = prepareShuffleDependency()
    preparePostShuffleRDD(shuffleDependency)
  }

  private[exchange] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeWithCustomPartitionerExec
      .prepareShuffleDependency(child.execute(), child.output, partitioning, serializer)
  }

  /**
   * Returns a [[ShuffledRowRDD]] that represents the post-shuffle dataset.
   * This [[ShuffledRowRDD]] is created based on a given [[ShuffleDependency]] and an optional
   * partition start indices array. If this optional array is defined, the returned
   * [[ShuffledRowRDD]] will fetch pre-shuffle partitions based on indices of this array.
   */
  private[exchange] def preparePostShuffleRDD(
                   shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
                   specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    //    // If an array of partition start indices is provided, we need to use this array
    //    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    //    // update the number of post-shuffle partitions.
    //    specifiedPartitionStartIndices.foreach { indices =>
    //      assert(newPartitioning.isInstanceOf[HashPartitioning])
    //      newPartitioning = UnknownPartitioning(indices.length)
    //    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }
}

object ShuffleExchangeWithCustomPartitionerExec {

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
             rdd: RDD[InternalRow],
             outputAttributes: Seq[Attribute],
             partitioning: Partitioning,
             serializer: Serializer): ShuffleDependency[Int, InternalRow, InternalRow] = {

    val partitioner: Partitioner = partitioning match {
      case customPartitioning: CustomPartitioning => customPartitioning.partitioner
      case _ => sys.error(s"Exchange not implemented for $partitioning")
    }

    def partitionKeyExtractor(): InternalRow => Any = partitioning match {
      case customPartitioning: CustomPartitioning => customPartitioning.getPartitionKey
      case _ => sys.error(s"Exchange not implemented for $partitioning")
    }

    val rddWithPartitionIds = if (needToCopyObjectsBeforeShuffle(partitioner)) {
      rdd.mapPartitionsWithIndexInternal { case (partition, iter) =>
        val getPartitionKey = partitionKeyExtractor()
        iter.map { row => (partitioner.getPartition(getPartitionKey(row)), row.copy()) }
      }
    } else {
      rdd.mapPartitionsWithIndexInternal { case (partition, iter) =>
        val getPartitionKey = partitionKeyExtractor()
        val mutablePair = new MutablePair[Int, InternalRow]()
        iter.map { row =>
          mutablePair.update(partitioner.getPartition(getPartitionKey(row)), row)
        }
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
    new ShuffleDependency[Int, InternalRow, InternalRow](
      rddWithPartitionIds,
      new PartitionIdPassthrough(partitioner.numPartitions),
      serializer)

    dependency
  }

  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }
}

