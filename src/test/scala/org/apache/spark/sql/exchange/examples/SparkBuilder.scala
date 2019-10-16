package org.apache.spark.sql.exchange.examples

import com.gelerion.spark.dataset.partitioner.RepartitionByCustomStrategy
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.internal.SQLConf

object SparkBuilder {

  def getSpark(): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("repartition-example")
      .master("local[1]")
      .config("spark.network.timeout", "10000001")
      .config("spark.executor.heartbeatInterval", "10000000")
      .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ColumnPruning.ruleName)
      .getOrCreate()

    spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil

    spark
  }

}
