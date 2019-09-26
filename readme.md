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