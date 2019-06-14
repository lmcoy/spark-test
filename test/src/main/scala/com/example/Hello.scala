package com.example

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Hello extends App {

  def square(x: Int): Int = x * x

  val config = ConfigFactory.load()

  val value = config.getInt("value")

  def f(o: Option[Int]) : Int = o.get

  println(BuildInfo)

  println(square(value))

  val input = Seq(
    Row(1, "test"),
    Row(5, "row2"),
    Row(4, "hallo")
  )


  val schema = StructType(
    Seq(
      StructField("integers", IntegerType),
      StructField("strings", StringType)
    )
  )

  val spark = SparkSession.builder
    .master("local")
    .appName("Hello")
    .getOrCreate()

  val df = spark.createDataFrame(spark.sparkContext.parallelize(input), schema)

  df.show()

}
