package org.apache.spark.sql.exchange.integration

object domain

case class Dept(id: String, value: String)
case class Emp(value: String, id: String)

case class JoinDept(department: String, gender: String)
case class DeptDetails(department: String, salary: String, age: Int)