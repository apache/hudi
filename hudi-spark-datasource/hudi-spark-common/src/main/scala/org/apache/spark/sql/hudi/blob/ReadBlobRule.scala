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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Transforms queries with `read_blob()` to use lazy batched I/O.
 *
 * Replaces [[ReadBlobExpression]] markers with [[BatchedBlobRead]] nodes
 * that read blob data during physical execution.
 *
 * Example: `SELECT id, read_blob(image_data) FROM table`
 *
 * @param spark SparkSession for accessing configuration
 */
case class ReadBlobRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case Project(projectList, Filter(condition, child))
      if containsReadBlobExpression(projectList)
        && containsReadBlobInExpression(condition)
        && !child.isInstanceOf[BatchedBlobRead] =>
      val projectBlobCols = extractAllBlobColumns(projectList)
      val filterBlobCols = extractBlobColumnsFromExpression(condition)
      val blobColumns = (projectBlobCols ++ filterBlobCols)
        .foldLeft((mutable.LinkedHashSet.empty[ExprId], ArrayBuffer.empty[AttributeReference])) {
          case ((seen, acc), a) if seen.add(a.exprId) => (seen, acc += a)
          case ((seen, acc), _) => (seen, acc)
        }._2.toSeq
      val (wrappedPlan, blobToDataAttr) = wrapWithBlobReads(blobColumns, child)
      val newCondition = replaceReadBlobExpression(condition, blobToDataAttr)
      val newProjectList = transformNamedExpressions(projectList, blobToDataAttr)
      Project(newProjectList, Filter(newCondition, wrappedPlan))

    case Filter(condition, child)
      if containsReadBlobInExpression(condition)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val blobColumns = extractBlobColumnsFromExpression(condition)
      val (wrappedPlan, blobToDataAttr) = wrapWithBlobReads(blobColumns, child)
      val newCondition = replaceReadBlobExpression(condition, blobToDataAttr)
      Project(child.output, Filter(newCondition, wrappedPlan))

    case Project(projectList, child)
      if containsReadBlobExpression(projectList)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val blobColumns = extractAllBlobColumns(projectList)
      val (wrappedPlan, blobToDataAttr) = wrapWithBlobReads(blobColumns, child)
      val newProjectList = transformNamedExpressions(projectList, blobToDataAttr)
      Project(newProjectList, wrappedPlan)

    case node if containsReadBlobInAnyExpression(node) =>
      throw new IllegalArgumentException(
        s"read_blob() may only appear in SELECT or WHERE clauses. Found in unsupported logical plan node: ${node.nodeName}. " +
        s"Move read_blob() to a SELECT or WHERE clause. Full plan: ${node.simpleStringWithNodeId()}")
  }

  private def containsReadBlobInAnyExpression(plan: LogicalPlan): Boolean = {
    plan.expressions.exists(containsReadBlobInExpression)
  }

  private def wrapWithBlobReads(
      blobColumns: Seq[AttributeReference],
      child: LogicalPlan): (LogicalPlan, Map[ExprId, Attribute]) = {
    if (blobColumns.isEmpty) {
      throw new IllegalStateException("read_blob() found but no valid blob column reference extracted.")
    }
    blobColumns.foldLeft((child: LogicalPlan, Map.empty[ExprId, Attribute])) {
      case ((currentPlan, mapping), blobAttr) =>
        // Type compatibility check (early fail for non-struct columns)
        blobAttr.dataType match {
          case struct: StructType if DataType.equalsIgnoreCaseAndNullability(struct, org.apache.spark.sql.types.BlobType.dataType) =>
            // Valid blob column
          case _ =>
            throw new IllegalArgumentException(
              s"Blob column '${blobAttr.name}' must be compatible with BlobType (type, data, reference struct), found: ${blobAttr.dataType}")
        }
        val blobRead = BatchedBlobRead(currentPlan, blobAttr)
        (blobRead, mapping + (blobAttr.exprId -> blobRead.dataAttr))
    }
  }

  private def extractBlobColumnsFromExpression(expr: Expression): Seq[AttributeReference] = {
    val seen = mutable.LinkedHashSet.empty[ExprId]
    val result = ArrayBuffer.empty[AttributeReference]
    collectBlobColumns(expr, seen, result)
    result.toSeq
  }

  /**
   * Check if any expression in the project list contains a ReadBlobExpression.
   */
  private def containsReadBlobExpression(projectList: Seq[Expression]): Boolean = {
    projectList.exists(expr => containsReadBlobInExpression(expr))
  }

  private def containsReadBlobInExpression(expr: Expression): Boolean = {
    expr match {
      case _: ReadBlobExpression => true
      case other => other.children.exists(containsReadBlobInExpression)
    }
  }

  private def extractAllBlobColumns(expressions: Seq[Expression]): Seq[AttributeReference] = {
    val seen = mutable.LinkedHashSet.empty[ExprId]
    val result = ArrayBuffer.empty[AttributeReference]
    expressions.foreach(collectBlobColumns(_, seen, result))
    result.toSeq
  }

  private def collectBlobColumns(
      expr: Expression,
      seen: mutable.Set[ExprId],
      result: ArrayBuffer[AttributeReference]): Unit = expr match {
    case ReadBlobExpression(attr: AttributeReference) =>
      if (seen.add(attr.exprId)) result += attr
    case other =>
      other.children.foreach(collectBlobColumns(_, seen, result))
  }

  private def transformNamedExpressions(
      expressions: Seq[NamedExpression],
      blobToDataAttr: Map[ExprId, Attribute]): Seq[NamedExpression] = {
    expressions.map {
      case alias @ Alias(childExpr, name) =>
        val rewritten = replaceReadBlobExpression(childExpr, blobToDataAttr)
        Alias(rewritten, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
      case attr: AttributeReference => attr
      case other =>
        replaceReadBlobExpression(other, blobToDataAttr).asInstanceOf[NamedExpression]
    }
  }

  private def replaceReadBlobExpression(
      expr: Expression,
      blobToDataAttr: Map[ExprId, Attribute]): Expression = expr match {
    case ReadBlobExpression(attr: AttributeReference) =>
      blobToDataAttr.getOrElse(attr.exprId, throw new IllegalArgumentException(
        s"read_blob() called on column '${attr.name}' (exprId=${attr.exprId}) which was not registered for blob reading. " +
        s"Available blob columns: ${blobToDataAttr.keys.mkString(", ")}"))
    case ReadBlobExpression(_) =>
      throw new IllegalStateException("read_blob() must be called on a direct column reference")
    case other =>
      other.mapChildren(replaceReadBlobExpression(_, blobToDataAttr))
  }
}
