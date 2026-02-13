/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.HoodieDatasetBulkInsertHelper.sparkAdapter
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hudi.blob.BatchedByteRangeReader.DATA_COL
import org.apache.spark.sql.hudi.expressions.ResolveBytesExpression
import org.slf4j.LoggerFactory

/**
 * Analyzer rule to resolve blob references efficiently using BatchedByteRangeReader.
 *
 * This rule detects queries containing [[ResolveBytesExpression]] markers (created by
 * the `resolve_bytes()` SQL function) and transforms the logical plan to use batched
 * I/O operations for efficient blob data reading.
 *
 * <h3>Transformation Process:</h3>
 * {{{
 * Before: Project([id, resolve_bytes(file_info) as data], child)
 * After:  Project([id, __temp__data as data], BatchedRead(child))
 * }}}
 *
 * <h3>How It Works:</h3>
 * <ol>
 *   <li>Detects [[Project]] nodes containing [[ResolveBytesExpression]] markers</li>
 *   <li>Applies [[BatchedByteRangeReader.readBatched()]] to the child DataFrame</li>
 *   <li>Replaces [[ResolveBytesExpression]] with references to the `__temp__data` column</li>
 *   <li>Returns transformed plan with efficient batched I/O</li>
 * </ol>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li>`hoodie.blob.batching.max.gap.bytes` (default: 4096) - Max gap between reads to batch</li>
 *   <li>`hoodie.blob.batching.lookahead.size` (default: 50) - Rows to buffer for batch detection</li>
 * </ul>
 *
 * @param spark SparkSession for accessing configuration
 */
case class ResolveBlobReferencesRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case Project(projectList, child)
      if containsResolveBytesExpression(projectList)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val blobColumn =
        extractBlobColumnFromExpressions(projectList).getOrElse {
          throw new IllegalStateException(
            "resolve_bytes() function found but no valid blob column reference could be extracted. " +
              "Ensure that resolve_bytes() is called on a BLOB type."
          )
        }

      val wrapped = BatchedBlobRead(child, blobColumn)

      val newProjectList =
        transformNamedExpressions(projectList, wrapped.dataAttr)

      Project(newProjectList, wrapped)
  }

  /**
   * Check if any expression in the project list contains a ResolveBytesExpression.
   */
  private def containsResolveBytesExpression(projectList: Seq[Expression]): Boolean = {
    projectList.exists(expr => containsResolveBytesInExpression(expr))
  }

  private def containsResolveBytesInExpression(expr: Expression): Boolean = {
    expr match {
      case _: ResolveBytesExpression => true
      case other => other.children.exists(containsResolveBytesInExpression)
    }
  }

  private def transformNamedExpressions(expressions: Seq[NamedExpression],
                                         dataAttr: Attribute): Seq[NamedExpression] = {

    expressions.map {

      case alias @ Alias(childExpr, name) =>
        val rewritten = replaceResolveBytesExpression(childExpr, dataAttr)
        Alias(rewritten, name)(
          alias.exprId,
          alias.qualifier,
          alias.explicitMetadata
        )

      case attr: AttributeReference =>
        // No remapping needed anymore
        attr

      case other =>
        replaceResolveBytesExpression(other, dataAttr)
          .asInstanceOf[NamedExpression]
    }
  }

  /**
   * Replace ResolveBytesExpression with reference to resolved data column.
   *
   * Recursively traverses expression tree and replaces all [[ResolveBytesExpression]]
   * nodes with references to the data column produced by [[BatchedByteRangeReader]].
   */
  private def replaceResolveBytesExpression(
      expr: Expression,
      dataAttr: Attribute): Expression = expr match {

    case ResolveBytesExpression(_) =>
      // Replace with reference to the data column
      dataAttr

    case other =>
      // Recursively process children
      other.mapChildren(child => replaceResolveBytesExpression(child, dataAttr))
  }

  /**
   * Extract blob column name from expressions (handles nesting in aggregate functions).
   */
  private def extractBlobColumnFromExpressions(expressions: Seq[Expression]): Option[AttributeReference] = {
    expressions.collectFirst {
      case expr if containsResolveBytesInExpression(expr) =>
        findBlobColumnName(expr)
    }.flatten
  }

  /**
   * Recursively search for ResolveBytesExpression and extract column name.
   */
  private def findBlobColumnName(expr: Expression): Option[AttributeReference] = expr match {
    case ResolveBytesExpression(child) =>
      child match {
        case attr: AttributeReference => Some(attr)
        case _ => None
      }
    case other =>
      other.children.flatMap(findBlobColumnName).headOption
  }
}
