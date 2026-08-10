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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{Decimal, NullType, NumericType, StringType}

object HoodieVectorSearchTableValuedFunction {

  val FUNC_NAME = "hudi_vector_search"

  object DistanceMetric extends Enumeration {
    val COSINE, L2, DOT_PRODUCT = Value

    def fromString(s: String): Value = Option(s).map(_.toLowerCase).getOrElse("") match {
      case "cosine" => COSINE
      case "l2" | "euclidean" => L2
      case "dot_product" | "inner_product" => DOT_PRODUCT
      case other => throw new HoodieAnalysisException(
        s"Unsupported distance metric: '$other'. Supported: cosine, l2, dot_product")
    }
  }

  object SearchAlgorithm extends Enumeration {
    val BRUTE_FORCE = Value

    def fromString(s: String): Value = Option(s).map(_.toLowerCase).getOrElse("") match {
      case "brute_force" => BRUTE_FORCE
      case other => throw new HoodieAnalysisException(
        s"Unsupported search algorithm: '$other'. Supported: brute_force")
    }
  }

  case class ParsedArgs(
    table: String,
    embeddingCol: String,
    queryVectorExpr: Expression,
    k: Int,
    metric: DistanceMetric.Value,
    algorithm: SearchAlgorithm.Value,
    filter: Option[String],
    maxDistance: Option[Double]
  )

  /**
   * Parse arguments for the hudi_vector_search TVF (single-query mode).
   *
   * Signature (4–8 args):
   *   hudi_vector_search('table', 'embedding_col', ARRAY(1.0, 2.0, ...), k
   *     [, 'metric'] [, 'algorithm'] [, 'filter_predicate' | NULL] [, max_distance | NULL])
   *   metric defaults to 'cosine'; algorithm defaults to 'brute_force'.
   *   filter is a SQL predicate applied to the corpus before distance computation;
   *     NULL, the empty string, and whitespace-only strings all mean "no filter."
   *   max_distance excludes results whose distance exceeds the given threshold;
   *     NULL means "no threshold." Must be a numeric literal when specified.
   */
  def parseArgs(exprs: Seq[Expression]): ParsedArgs = {
    if (exprs.size < 4 || exprs.size > 8) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 4-8 arguments: " +
          "(table, embedding_col, query_vector, k [, metric] [, algorithm] [, filter] [, max_distance]).")
    }

    def requireStringLiteral(expr: Expression, argName: String): String = expr match {
      case Literal(v, StringType) if v != null => v.toString
      case _ => throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME': argument '$argName' must be a string literal, got: ${expr.sql}")
    }

    val table = requireStringLiteral(exprs.head, "table")
    val embeddingCol = requireStringLiteral(exprs(1), "embedding_col")
    val queryVectorExpr = exprs(2)
    val k = parseK(FUNC_NAME, exprs(3))
    val metric = if (exprs.size >= 5) DistanceMetric.fromString(requireStringLiteral(exprs(4), "metric"))
    else DistanceMetric.COSINE
    val algorithm = if (exprs.size >= 6) SearchAlgorithm.fromString(requireStringLiteral(exprs(5), "algorithm"))
    else SearchAlgorithm.BRUTE_FORCE
    val filter = if (exprs.size >= 7) parseOptionalString(FUNC_NAME, exprs(6), "filter") else None
    val maxDistance = if (exprs.size >= 8) parseOptionalDouble(FUNC_NAME, exprs(7), "max_distance") else None
    ParsedArgs(table, embeddingCol, queryVectorExpr, k, metric, algorithm, filter, maxDistance)
  }

  private[logical] def parseK(funcName: String, expr: Expression): Int = {
    val rawValue = expr.eval()
    if (rawValue == null) {
      throw new HoodieAnalysisException(
        s"Function '$funcName': k must be a positive integer, got null")
    }
    val kValue = try {
      rawValue.toString.toInt
    } catch {
      case _: NumberFormatException =>
        throw new HoodieAnalysisException(
          s"Function '$funcName': k must be a positive integer, got '$rawValue'")
    }
    if (kValue <= 0) {
      throw new HoodieAnalysisException(
        s"Function '$funcName': k must be a positive integer, got $kValue")
    }
    kValue
  }

  /** Parses a string argument that may be NULL (meaning "not specified"). */
  private[logical] def parseOptionalString(
      funcName: String, expr: Expression, argName: String): Option[String] = expr match {
    case Literal(null, _) => None
    case Literal(v, StringType) if v != null =>
      val s = v.toString.trim
      if (s.isEmpty) None else Some(s)
    case _ => throw new HoodieAnalysisException(
      s"Function '$funcName': argument '$argName' must be a string literal or NULL, got: ${expr.sql}")
  }

  /**
   * Parses a numeric argument that may be NULL (meaning "not specified"). Accepts
   * any foldable expression of [[NumericType]] (or [[NullType]] for an untyped
   * NULL keyword) — including a bare literal or {@code CAST(literal AS numeric)} —
   * and widens to Double. String literals are rejected even when their contents
   * happen to parse as a number, so the type contract surfaces at parse time.
   */
  private[logical] def parseOptionalDouble(
      funcName: String, expr: Expression, argName: String): Option[Double] = {
    val numericOrNull = expr.dataType match {
      case _: NumericType | NullType => true
      case _ => false
    }
    if (!expr.foldable || !numericOrNull) {
      throw new HoodieAnalysisException(
        s"Function '$funcName': argument '$argName' must be a numeric literal or NULL, got: ${expr.sql}")
    }
    Option(expr.eval()).map {
      case d: Decimal => d.toDouble
      case n: Number => n.doubleValue()
      case other => throw new HoodieAnalysisException(
        s"Function '$funcName': argument '$argName' has unexpected runtime type: " +
          s"${other.getClass.getName}")
    }
  }
}

/**
 * Unresolved logical plan node for the {@code hudi_vector_search} table-valued function
 * (single-query mode). Replaced during analysis by the resolved search plan.
 *
 * <p><b>Usage (SQL):</b>
 * {{{
 *   SELECT * FROM hudi_vector_search('table_name', 'embedding_col', ARRAY(1.0, 2.0, 3.0), 10)
 *   SELECT * FROM hudi_vector_search('table_name', 'embedding_col', ARRAY(1.0, 2.0, 3.0), 10, 'cosine', 'brute_force')
 * }}}
 *
 * <p><b>Output columns:</b> all corpus columns (minus embedding) + {@code _hudi_distance: Double}.
 * Results are ordered by distance ascending (lower = more similar), limited to top-k.
 *
 * <p><b>Type matching:</b> the corpus embedding column and query vector must have compatible
 * element types (e.g. both float or both double). Mismatched types produce an error.
 */
case class HoodieVectorSearchTableValuedFunction(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false
}

object HoodieVectorSearchBatchTableValuedFunction {

  val FUNC_NAME = "hudi_vector_search_batch"

  case class ParsedArgs(
    corpusTable: String,
    corpusEmbeddingCol: String,
    queryTable: String,
    queryEmbeddingCol: String,
    k: Int,
    metric: HoodieVectorSearchTableValuedFunction.DistanceMetric.Value,
    algorithm: HoodieVectorSearchTableValuedFunction.SearchAlgorithm.Value,
    filter: Option[String],
    maxDistance: Option[Double]
  )

  /**
   * Parse arguments for the hudi_vector_search_batch TVF (batch-query mode).
   *
   * Signature (5–9 args):
   *   hudi_vector_search_batch('corpus_table', 'corpus_col', 'query_table', 'query_col', k
   *     [, 'metric'] [, 'algorithm'] [, 'filter_predicate' | NULL] [, max_distance | NULL])
   *   metric defaults to 'cosine'; algorithm defaults to 'brute_force'.
   *   filter is a SQL predicate applied to the corpus before distance computation;
   *     NULL, the empty string, and whitespace-only strings all mean "no filter."
   *   max_distance excludes results whose distance exceeds the given threshold;
   *     NULL means "no threshold." Must be a numeric literal when specified.
   */
  def parseArgs(exprs: Seq[Expression]): ParsedArgs = {
    if (exprs.size < 5 || exprs.size > 9) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 5-9 arguments: " +
          "(corpus_table, corpus_col, query_table, query_col, k [, metric] [, algorithm] [, filter] [, max_distance]).")
    }

    def requireStringLiteral(expr: Expression, argName: String): String = expr match {
      case Literal(v, StringType) if v != null => v.toString
      case _ => throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME': argument '$argName' must be a string literal, got: ${expr.sql}")
    }

    val corpusTable = requireStringLiteral(exprs.head, "corpus_table")
    val corpusEmbeddingCol = requireStringLiteral(exprs(1), "corpus_col")
    val queryTable = requireStringLiteral(exprs(2), "query_table")
    val queryEmbeddingCol = requireStringLiteral(exprs(3), "query_col")
    val k = HoodieVectorSearchTableValuedFunction.parseK(FUNC_NAME, exprs(4))
    val metric = if (exprs.size >= 6)
      HoodieVectorSearchTableValuedFunction.DistanceMetric.fromString(requireStringLiteral(exprs(5), "metric"))
    else HoodieVectorSearchTableValuedFunction.DistanceMetric.COSINE
    val algorithm = if (exprs.size >= 7)
      HoodieVectorSearchTableValuedFunction.SearchAlgorithm.fromString(requireStringLiteral(exprs(6), "algorithm"))
    else HoodieVectorSearchTableValuedFunction.SearchAlgorithm.BRUTE_FORCE
    val filter = if (exprs.size >= 8)
      HoodieVectorSearchTableValuedFunction.parseOptionalString(FUNC_NAME, exprs(7), "filter")
    else None
    val maxDistance = if (exprs.size >= 9)
      HoodieVectorSearchTableValuedFunction.parseOptionalDouble(FUNC_NAME, exprs(8), "max_distance")
    else None
    ParsedArgs(corpusTable, corpusEmbeddingCol, queryTable, queryEmbeddingCol,
      k, metric, algorithm, filter, maxDistance)
  }
}

/**
 * Unresolved logical plan node for the {@code hudi_vector_search_batch} table-valued function
 * (batch-query mode). Replaced during analysis by the resolved search plan.
 *
 * <p><b>Usage (SQL):</b>
 * {{{
 *   SELECT * FROM hudi_vector_search_batch('corpus_table', 'corpus_col', 'query_table', 'query_col', 5)
 *   SELECT * FROM hudi_vector_search_batch('corpus_table', 'corpus_col', 'query_table', 'query_col', 5, 'cosine')
 * }}}
 *
 * <p><b>Output columns:</b> all corpus columns (minus embedding) + {@code _hudi_distance: Double}
 * + {@code _hudi_query_index: Long} + query table columns (clashing names prefixed with
 * {@code _hudi_query_}). Results are partitioned by query and ordered by distance ascending,
 * top-k per query.
 */
case class HoodieVectorSearchBatchTableValuedFunction(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false
}
