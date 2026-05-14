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

import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.model.HoodieFileFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedFileFormat
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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val transformed = plan resolveOperatorsUp {
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
    injectForceContentColumnOptions(transformed)
  }

  /**
   * For every [[BatchedBlobRead]] in the transformed plan, find the underlying Hudi Parquet
   * [[LogicalRelation]] that produces its blob attribute and add an internal option carrying the
   * set of blob column names that must keep their `data` sub-field at read time. This lets
   * read_blob() materialize bytes per-column even when the relation was constructed in
   * `hoodie.read.blob.inline.mode=DESCRIPTOR`, without mutating any shared FileFormat state.
   *
   * Lance and non-Parquet formats are skipped — Lance handles DESCRIPTOR + read_blob() natively
   * via its populated reference field. Relations not in DESCRIPTOR mode at construction time are
   * also skipped (no strip happens, so no override is needed).
   */
  private def injectForceContentColumnOptions(plan: LogicalPlan): LogicalPlan = {
    val readBlobAttrIds: Set[ExprId] = plan.collect {
      case BatchedBlobRead(_, attr, _) => attr.exprId
    }.toSet
    if (readBlobAttrIds.isEmpty) {
      plan
    } else {
      plan transformDown {
        case lr @ LogicalRelation(rel: HadoopFsRelation, _, _, _) =>
          rel.fileFormat match {
            case ff: HoodieFileGroupReaderBasedFileFormat
                if ff.hoodieFileFormat == HoodieFileFormat.PARQUET && ff.isBlobDescriptorMode =>
              val matched: Set[String] = lr.output.collect {
                case a: AttributeReference if readBlobAttrIds.contains(a.exprId) => a.name
              }.toSet
              if (matched.isEmpty) {
                lr
              } else {
                val newOptions = rel.options +
                  (HoodieReaderConfig.BLOB_INLINE_READ_FORCE_CONTENT_COLUMNS -> matched.mkString(","))
                val newRel = HadoopFsRelation(
                  location = rel.location,
                  partitionSchema = rel.partitionSchema,
                  dataSchema = rel.dataSchema,
                  bucketSpec = rel.bucketSpec,
                  fileFormat = rel.fileFormat,
                  options = newOptions)(rel.sparkSession)
                lr.copy(relation = newRel)
              }
            case _ => lr
          }
      }
    }
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
