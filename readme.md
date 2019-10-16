# Intention

# Usage
Configuration step
```scala
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning

val conf = new SparkConf()
  //This is a must! See explanation under Limitations section
  .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ColumnPruning.ruleName)

val spark = SparkSession.builder().config(conf).getOrCreate()

//All the magic happens here
spark.experimental.extraStrategies = RepartitionByCustomStrategy :: Nil
```
  
#### Using with `Dataset`

- Create custom `Dataset`
```scala
case class Person(name: String, age: Int, id: String)
val persons: Dataset[Person] = spark.createDataset(makePersons())
```

- Define custom partitioner
```scala
import org.apache.spark.sql.exchange.repartition.partitioner.TypedPartitioner

class PersonByFirstCharPartitioner extends TypedPartitioner[Person] {
  override def getPartitionIdx(person: Person): Int = if (person.name.startsWith("D")) 0 else 1
  override def numPartitions: Int = 2
}
```
There are several differences between built-in Spark partitioner:
1. `TypedPartitioner` is a type-safe partitioner with compile time validation. 
2. It takes whole row as input, not the key

- Repartition our data
```scala
//adds repartitionBy method to the Dataset API
import org.apache.spark.sql.exchange.implicits._

val partitioned = persons.repartitionBy(new PersonByFirstCharPartitioner)
```
As easy as it looks!  
  
Lets check the distribution
```scala
println(partitioned.rdd.getNumPartitions)
//2
println(partitioned.rdd.glom().collect() ...)
//partition #0: Person(Denis,0,20780),Person(Denis,0,28867),Person(Denis,0,10671),Person(Denis,0,6121)
//partition #1: Person(Max,0,7920),Person(Eitan,0,19589),Person(Viktor,0,10803),Person(Marat,0,12)
```   
As we could see partition `0` composed of persons with name starts with `D`.
    
Check `org.apache.spark.sql.exchange.examples.RepartitionPersonsByNameDataset` for complete example

#### Using with `Dataframe`
The single difference is in a partitioner type
```scala
val partitioned = departments.repartitionBy(new RowPartitioner {
   override def getPartitionIdx(row: Row): Int =
      if (row.getAs[String]("id").startsWith("d")) 0 else 1
   override def numPartitions: Int = 2
})
```
For untyped `Datafreame` we use `RowPartitioner` instead.
  
Check `org.apache.spark.sql.exchange.examples.RepartitionRowsByKeyDataframe` for complete example

## GroupBy 

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