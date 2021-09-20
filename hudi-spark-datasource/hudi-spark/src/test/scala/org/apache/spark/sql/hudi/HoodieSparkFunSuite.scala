/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import java.io.PrintWriter
import java.nio.charset.StandardCharsets.UTF_8
import java.util.TimeZone

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

/**
 * This code is mainly copy from Spark 2.X (org.apache.spark.sql.QueryTest).
 */
trait HoodieSparkFunSuite extends FunSuite {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
   val analyzedDF = try df catch {
    case ae: AnalysisException =>
     if (ae.plan.isDefined) fail(
      s"""
         |Failed to analyze query: $ae
         |${ae.plan.get}
         |
               |${stackTraceToString(ae)}
         |""".stripMargin) else throw ae
   }

   assertEmptyMissingInput(analyzedDF)

   SparkFunSuite.checkAnswer(analyzedDF, expectedAnswer) match {
    case Some(errorMessage) => fail(errorMessage)
    case None =>
   }
  }

  def stackTraceToString(t: Throwable): String = {
   val out = new java.io.ByteArrayOutputStream
   Utils.tryWithResource(new PrintWriter(out)) { writer =>
    t.printStackTrace(writer)
    writer.flush()
   }
   new String(out.toByteArray, UTF_8)
  }

  /**
   * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
   */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
   assert(query.queryExecution.analyzed.missingInput.isEmpty,
    s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
   assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
    s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
   assert(query.queryExecution.executedPlan.missingInput.isEmpty,
    s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }
}

object SparkFunSuite {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   *HoodieSpark2ExtendedSqlParser.scala
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      df.rdd.count()  // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        // scalastyle:off
        return Some(errorMessage)
        // scalastyle:on
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
        |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def genError(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row.map(row =>
        if (row.schema == null) {
          "struct<>"
        } else {
          s"${row.schema.catalogString}"
        }).getOrElse("struct<>")

    s"""
       |== Results ==
       |${
      sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
        getRowType(expectedAnswer.headOption) +:
        prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
        getRowType(sparkAnswer.headOption) +:
        prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")
    }
    """.stripMargin
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
      Some(genError(expectedAnswer, sparkAnswer, isSorted))
    } else {
      None
    }
  }
}
