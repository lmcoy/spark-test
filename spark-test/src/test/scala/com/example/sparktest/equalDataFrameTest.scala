package com.example.sparktest

import com.example.sparktest.equalDataFrame.Tolerance
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.MatchResult
import org.scalatest.{FlatSpec, Matchers}

class equalDataFrameTest extends FlatSpec with Matchers with DataFrameSuiteBase {

  "equalDataFrame" should "match if the DataFrames are equal" in {

    val input1 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo")
    )

    val input2 = Seq(
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

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema)

    equalDataFrame(df2).apply(df1) should equal(
      MatchResult(matches = true, "DataFrames are not equal", "DataFrames are equal")
    )
  }

  it should "not match if the schema is not the same" in {
    val input1 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo")
    )

    val input2 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo")
    )

    val schema1 = StructType(
      Seq(
        StructField("integers", IntegerType),
        StructField("strings", StringType)
      )
    )

    val schema2 = StructType(
      Seq(
        StructField("integers", IntegerType),
        StructField("strings2", StringType)
      )
    )

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema1)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema2)

    val expected = MatchResult(
      matches = false,
      "the schemas don't match:\n" +
        "columns in left but not in right: Set(StructField(strings,StringType,true))\n" +
        "columns in right but not in left: Set(StructField(strings2,StringType,true))",
      "the schemas match"
    )

    equalDataFrame(df2).apply(df1) should equal(expected)
  }

  it should "not match if the DataFrames are not equal" in {

    val input1 = Seq(
      Row(1, "test"),
      Row(3, "row2"),
      Row(4, "hallo")
    )

    val input2 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo2")
    )

    val schema = StructType(
      Seq(
        StructField("integers", IntegerType),
        StructField("strings", StringType)
      )
    )

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema)

    val expected = MatchResult(matches = false,"DataFrames are not equal (2 rows are not equal):\n" +
      "row 1: [3,row2] =!= [5,row2]\n" +
    "row 2: [4,hallo] =!= [4,hallo2]","DataFrames are equal")

    equalDataFrame(df2).apply(df1) should equal(
      expected
    )
  }

  it should "not match and fail fast when the number of rows is not equal" in {

    val input1 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo")
    )

    val input2 = Seq(
      Row(1, "test"),
      Row(5, "row2"),
      Row(4, "hallo"),
      Row(10, "xxx")
    )

    val schema = StructType(
      Seq(
        StructField("integers", IntegerType),
        StructField("strings", StringType)
      )
    )

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema)

    equalDataFrame(df2).apply(df1) should equal(
      MatchResult(matches = false, "number of rows not equal: 3 != 4", "number of rows equal")
    )
  }

  it should "not match and print up to 10 non matching rows" in {

    val input1 = Seq(
      Row(1, "test"),
      Row(2, "test"),
      Row(3, "test"),
      Row(4, "test"),
      Row(5, "test"),
      Row(6, "test"),
      Row(7, "test"),
      Row(8, "test"),
      Row(9, "test"),
      Row(10, "test"),
      Row(11, "test"),
      Row(12, "test")
    )

    val input2 = Seq(
      Row(1, "test2"),
        Row(2, "test2"),
        Row(3, "test2"),
        Row(4, "test2"),
        Row(5, "test2"),
        Row(6, "test2"),
        Row(7, "test2"),
        Row(8, "test2"),
        Row(9, "test2"),
        Row(10, "test2"),
        Row(11, "test2"),
        Row(12, "test2")
    )

    val schema = StructType(
      Seq(
        StructField("integers", IntegerType),
        StructField("strings", StringType)
      )
    )

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema)

    val result = equalDataFrame(df2).apply(df1)

    result.failureMessage should startWith("DataFrames are not equal (showing 10 of 12 non matching rows):")
    result.failureMessage.lines.size should equal (11)
  }

  it should "match if Doubles are within tolerance" in {

    val input1 = Seq(
      Row(0.1),
      Row(0.01),
      Row(2.3)
    )

    val input2 = Seq(
      Row(0.1001),
      Row(0.0101),
      Row(2.30001)
    )

    val schema = StructType(
      Seq(
        StructField("doubles", DoubleType)
      )
    )

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(input1), schema)
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(input2), schema)

    equalDataFrame(df2)(Tolerance(0.001)).apply(df1) should equal(
      MatchResult(matches = true, "DataFrames are not equal", "DataFrames are equal")
    )
  }

}
