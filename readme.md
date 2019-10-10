# Intention


# Usage

# Limitations

# Reference Project
How partitioner works in plain Spark?
```
val dep_1_rdd = dep_1.rdd.map(dept => (dept.id, dept)).partitionBy(new Partitioner {
  override def numPartitions: Int = 2
  override def getPartition(key: Any): Int = if (key.asInstanceOf[String].startsWith("a")) 0 else 1
})

val dep_2_rdd = dep_2.rdd.map(dept => (dept.id, dept)).partitionBy(new Partitioner {
  override def numPartitions: Int = 2
  override def getPartition(key: Any): Int = if (key.asInstanceOf[String].startsWith("a")) 0 else 1
}

dep_1_rdd.join(dep_2_rdd).collect().foreach(println)
```

Well, we have two different instances of partitioner, so Spark will Shuffle the data with the first rdd partitioner.

```
val byIdPartitioner = new Partitioner {
  override def numPartitions: Int = 2
  override def getPartition(key: Any): Int = if (key.asInstanceOf[String].startsWith("a")) 0 else 1
}
val dep_1_rdd = dep_1.rdd.map(dept => (dept.id, dept)).partitionBy(byIdPartitioner)
val dep_2_rdd = dep_2.rdd.map(dept => (dept.id, dept)).partitionBy(byIdPartitioner)

dep_1_rdd.join(dep_2_rdd).collect().foreach(println)
```
In this case Spark will properly identify the same partitioner

Limitations:
```scala
val dep_1 = department.repartitionBy(depPartitioner)
val dep_2 = department.repartition(2, $"id")
```
Wrong results, when several partitioners having the same field name and partition

join -> agg have to use .cache()

It is vital to disable `ColumnPruning` as it modifies datasource 
As it could modify the source such that an typed partitioner won;t be able to re-create an original object
Example
```
val joined = left.join(right, Seq("department"))
val grouped = joined.groupBy($"department").agg(sum($"salary").as("sum"))
grouped.explain()
```

A simplified execution plan with `disabled` column pruning 
```
+- Project [department, gender, salary, age]
  +- Exchange (custom partitioner) InternalTypedPartitioning
  :     LocalTableScan [department, salary, age]
  +- Exchange (custom partitioner) InternalTypedPartitioning
        LocalTableScan [department#2, gender#3]
```

A simplified execution plan with `enabled` column pruning 
```
+- Project [department, gender, salary, age]
  +- Exchange (custom partitioner) InternalTypedPartitioning
  :  +- Project [department, salary]
  :     LocalTableScan [department, salary, age]
  +- Exchange (custom partitioner) InternalTypedPartitioning
     +- Project [department] 
        +- LocalTableScan [department#2, gender#3]
```
Note the project being pushed, this will prevent

Don't mix partitioners with the same partition key and num partitions!
```
val data_1 = leftDS.repartitionBy(new TypedPartitioner {
      override def numPartitions: Int = 2
      override def partitionKeys: Option[Set[PartitionKey]] = Some(Set(("department", StringType)))
})
val data_2 = rightDS.repartition(2, $"department")
```
As both are repartition bu `department` into `two` partitions this will lead to the wrong results.
If you want to mix for some reason, use different numPart