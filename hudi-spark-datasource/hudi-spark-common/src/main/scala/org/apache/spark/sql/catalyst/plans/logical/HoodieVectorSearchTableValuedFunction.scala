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
  private[sql] val BRUTE_FORCE_FILTER_OPT = "_hudi.vector.brute_force.filter"
  private[sql] val BRUTE_FORCE_MAX_DISTANCE_OPT = "_hudi.vector.brute_force.max_distance"

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
    val BRUTE_FORCE, IVF_RABITQ_MDT = Value

    def fromString(s: String): Value = Option(s).map(_.toLowerCase).getOrElse("") match {
      case "brute_force" => BRUTE_FORCE
      case "ivf_rabitq_mdt" => IVF_RABITQ_MDT
      case other => throw new HoodieAnalysisException(
        s"Unsupported search algorithm: '$other'. Supported: brute_force, ivf_rabitq_mdt")
    }
  }

  case class ParsedArgs(
    table: String,
    embeddingCol: String,
    queryVectorExpr: Expression,
    k: Int,
    metric: DistanceMetric.Value,
    algorithm: SearchAlgorithm.Value,
    runtimeOptions: Map[String, String] = Map.empty
  )

  /**
   * Parse arguments for the hudi_vector_search TVF (single-query mode).
   *
   * Signature (4–8 args):
   *   hudi_vector_search('table', 'embedding_col', ARRAY(1.0, 2.0, ...), k
   *     [, 'metric'] [, 'algorithm'] [, algorithm_arg | NULL] [, max_distance | NULL])
   *   For brute_force, algorithm_arg is the optional SQL filter predicate and the eighth argument
   *   is max_distance. For ivf_rabitq_mdt, algorithm_arg is a comma-separated key=value option list
   *   and an eighth argument is not accepted.
   */
  def parseArgs(exprs: Seq[Expression]): ParsedArgs = {
    if (exprs.size < 4 || exprs.size > 8) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 4-8 arguments: " +
          "(table, embedding_col, query_vector, k [, metric] [, algorithm] [, algorithm_arg] [, max_distance]).")
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
    val runtimeOptions = algorithm match {
      case SearchAlgorithm.BRUTE_FORCE =>
        val filter = if (exprs.size >= 7) parseOptionalString(FUNC_NAME, exprs(6), "filter") else None
        val maxDistance = if (exprs.size >= 8) parseOptionalDouble(FUNC_NAME, exprs(7), "max_distance") else None
        filter.map(BRUTE_FORCE_FILTER_OPT -> _).toMap ++
          maxDistance.map(value => BRUTE_FORCE_MAX_DISTANCE_OPT -> value.toString).toMap
      case SearchAlgorithm.IVF_RABITQ_MDT =>
        if (exprs.size >= 8) {
          throw new HoodieAnalysisException(
            s"Function '$FUNC_NAME': max_distance is supported only by brute_force")
        }
        if (exprs.size >= 7) parseKvOptions(requireStringLiteral(exprs(6), "options"))
        else Map.empty[String, String]
    }
    ParsedArgs(table, embeddingCol, queryVectorExpr, k, metric, algorithm, runtimeOptions)
  }

  /** Parses a string argument that may be NULL (meaning "not specified"). */
  private[logical] def parseOptionalString(
      funcName: String, expr: Expression, argName: String): Option[String] = expr match {
    case Literal(null, _) => None
    case Literal(v, StringType) if v != null =>
      val value = v.toString.trim
      if (value.isEmpty) None else Some(value)
    case _ => throw new HoodieAnalysisException(
      s"Function '$funcName': argument '$argName' must be a string literal or NULL, got: ${expr.sql}")
  }

  /** Parses a foldable numeric argument that may be NULL and widens it to Double. */
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
      case value: Decimal => value.toDouble
      case value: Number => value.doubleValue()
      case other => throw new HoodieAnalysisException(
        s"Function '$funcName': argument '$argName' has unexpected runtime type: ${other.getClass.getName}")
    }
  }

  private[logical] def parseKvOptions(raw: String): Map[String, String] = {
    if (raw == null || raw.trim.isEmpty) {
      Map.empty
    } else {
      raw.split(",").map(_.trim).filter(_.nonEmpty).map { kv =>
        val idx = kv.indexOf('=')
        if (idx <= 0) throw new HoodieAnalysisException(
          s"Function '$FUNC_NAME': malformed option '$kv', expected key=value")
        (kv.substring(0, idx).trim, kv.substring(idx + 1).trim)
      }.toMap
    }
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
    runtimeOptions: Map[String, String] = Map.empty
  )

  /**
   * Parse arguments for the hudi_vector_search_batch TVF (batch-query mode).
   *
   * Signature (5–9 args):
   *   hudi_vector_search_batch('corpus_table', 'corpus_col', 'query_table', 'query_col', k
   *     [, 'metric'] [, 'algorithm'] [, algorithm_arg | NULL] [, max_distance | NULL])
   *   For brute_force, algorithm_arg is the optional SQL filter predicate and the ninth argument
   *   is max_distance. For ivf_rabitq_mdt, algorithm_arg is a comma-separated key=value option list.
   */
  def parseArgs(exprs: Seq[Expression]): ParsedArgs = {
    if (exprs.size < 5 || exprs.size > 9) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 5-9 arguments: " +
          "(corpus_table, corpus_col, query_table, query_col, k [, metric] [, algorithm] [, algorithm_arg] [, max_distance]).")
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
    val runtimeOptions = algorithm match {
      case HoodieVectorSearchTableValuedFunction.SearchAlgorithm.BRUTE_FORCE =>
        val filter = if (exprs.size >= 8)
          HoodieVectorSearchTableValuedFunction.parseOptionalString(FUNC_NAME, exprs(7), "filter")
        else None
        val maxDistance = if (exprs.size >= 9)
          HoodieVectorSearchTableValuedFunction.parseOptionalDouble(FUNC_NAME, exprs(8), "max_distance")
        else None
        filter.map(HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_FILTER_OPT -> _).toMap ++
          maxDistance.map(value =>
            HoodieVectorSearchTableValuedFunction.BRUTE_FORCE_MAX_DISTANCE_OPT -> value.toString).toMap
      case HoodieVectorSearchTableValuedFunction.SearchAlgorithm.IVF_RABITQ_MDT =>
        if (exprs.size >= 9) {
          throw new HoodieAnalysisException(
            s"Function '$FUNC_NAME': max_distance is supported only by brute_force")
        }
        if (exprs.size >= 8)
          HoodieVectorSearchTableValuedFunction.parseKvOptions(requireStringLiteral(exprs(7), "options"))
        else Map.empty[String, String]
    }
    ParsedArgs(corpusTable, corpusEmbeddingCol, queryTable, queryEmbeddingCol,
      k, metric, algorithm, runtimeOptions)
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
