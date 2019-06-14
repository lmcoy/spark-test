package com.example.sparktest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.{MatchResult, Matcher}
import com.example.sparktest.equalDataFrame.Tolerance
import com.holdenkarau.spark.testing.DataFrameSuiteBase

case class equalDataFrame(right: DataFrame)(implicit tol: Tolerance = Tolerance(0))
    extends Matcher[DataFrame]
    with Serializable {

  private def zipWithIndex[U](rdd: RDD[U]) =
    rdd.zipWithIndex().map { case (row, idx) => (idx, row) }

  private def mismatchMessage(schemaA: StructType, schemaB: StructType): String = {
    val setA = Set(schemaA.fields: _*)
    val setB = Set(schemaB.fields: _*)

    s"columns in left but not in right: ${setA.diff(setB)}\ncolumns in right but not in left: ${setB.diff(setA)}"
  }

  private def failureMessageForUnequalDataframes(unequalRows: Array[String],
                                                 numberUnequalRows: Long) = {
    val msg =
      if (unequalRows.length == numberUnequalRows)
        s"$numberUnequalRows rows are not equal"
      else
        s"showing ${unequalRows.length} of $numberUnequalRows non matching rows"
    s"DataFrames are not equal ($msg):${unequalRows.mkString("\n", "\n", "")}"
  }

  private def compareDataFramesWithSameSchema(left: DataFrame) =
    try {
      left.rdd.cache()
      right.rdd.cache()

      val leftCount  = left.rdd.count()
      val rightCount = right.rdd.count()

      if (leftCount != rightCount)
        MatchResult(matches = false,
                    s"number of rows not equal: $leftCount != $rightCount",
                    "number of rows equal")
      else {
        val leftIndexed  = zipWithIndex(left.rdd)
        val rightIndexed = zipWithIndex(right.rdd)

        val unequalRDD: RDD[(Long, (Row, Row))] = leftIndexed.join(rightIndexed).filter {
          case (_, (r1, r2)) =>
            !(r1.equals(r2) || DataFrameSuiteBase.approxEquals(r1, r2, tol.tol))
        }

        val numberUnequalRows = unequalRDD.count()
        if (numberUnequalRows == 0)
          MatchResult(matches = true, "DataFrames are not equal", "DataFrames are equal")
        else {
          val unequalRows = unequalRDD
            .take(10)
            .map(row => s"row ${row._1}: ${row._2._1.toString()} =!= ${row._2._2.toString()}")

          MatchResult(matches = false,
                      failureMessageForUnequalDataframes(unequalRows, numberUnequalRows),
                      "DataFrames are equal")
        }

      }
    } finally {
      left.rdd.unpersist()
      right.rdd.unpersist()
    }

  override def apply(left: DataFrame): MatchResult =
    if (left.schema != right.schema)
      MatchResult(matches = false,
                  s"the schemas don't match:\n${mismatchMessage(left.schema, right.schema)}",
                  "the schemas match")
    else compareDataFramesWithSameSchema(left)

}

object equalDataFrame {
  case class Tolerance(tol: Double)
}
