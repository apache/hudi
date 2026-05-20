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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
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
      rejectMixedBlobProjection(projectList, child)
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

      rejectMixedBlobProjection(projectList, child)
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

  /**
   * Reject queries that mix `read_blob(blob_col)` with a direct projection of any blob
   * column when the effective inline read mode resolves to DESCRIPTOR.
   */
  private def rejectMixedBlobProjection(
      projectList: Seq[Expression],
      child: LogicalPlan): Unit = {
    val directBlobRefs = collectDirectBlobRefs(projectList)
    val readBlobCols = extractAllBlobColumns(projectList)
    if (directBlobRefs.nonEmpty && readBlobCols.nonEmpty && effectiveModeIsDescriptor(child)) {
      val readBlobNames = readBlobCols.map(_.name).distinct.mkString(", ")
      val directNames = directBlobRefs.map(_.name).distinct.mkString(", ")
      throw new AnalysisException(
        s"Cannot mix read_blob() with direct blob-column projection under " +
          s"${HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()}=" +
          s"${HoodieReaderConfig.BLOB_INLINE_READ_MODE_DESCRIPTOR}. read_blob() is " +
          s"called on [$readBlobNames] while [$directNames] is projected directly in " +
          s"the same query. Either set " +
          s"${HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()}=" +
          s"${HoodieReaderConfig.BLOB_INLINE_READ_MODE_CONTENT} (returns inline bytes " +
          s"for all INLINE rows), or remove the direct blob-column projection from " +
          s"this query.")
    }
  }

  /**
   * Collect all blob columns referenced directly (i.e. NOT inside a `read_blob()`) in
   * the given expression list. A blob column is an `AttributeReference` whose dataType
   * is the `BlobType` struct.
   */
  private def collectDirectBlobRefs(
      expressions: Seq[Expression]): Seq[AttributeReference] = {
    val seen = mutable.LinkedHashSet.empty[ExprId]
    val result = ArrayBuffer.empty[AttributeReference]
    expressions.foreach(walkDirectBlobRefs(_, seen, result))
    result.toSeq
  }

  private def walkDirectBlobRefs(
      expr: Expression,
      seen: mutable.Set[ExprId],
      result: ArrayBuffer[AttributeReference]): Unit = expr match {
    // Skip subtrees rooted at read_blob() — its argument is wrapped, not direct.
    case _: ReadBlobExpression =>
    case attr: AttributeReference if isBlobAttr(attr) =>
      if (seen.add(attr.exprId)) result += attr
    case other =>
      other.children.foreach(walkDirectBlobRefs(_, seen, result))
  }

  private def isBlobAttr(attr: AttributeReference): Boolean = attr.dataType match {
    case struct: StructType =>
      DataType.equalsIgnoreCaseAndNullability(struct, org.apache.spark.sql.types.BlobType.dataType)
    case _ => false
  }

  /**
   * Resolve the effective value of `hoodie.read.blob.inline.mode` at plan time and
   * return true if it is DESCRIPTOR.
   *
   * Precedence (highest first): per-read DataFrame option on a `HadoopFsRelation` →
   * Spark session conf → catalog-table property (when present) → default (DESCRIPTOR).
   * The per-read option captures `.option("hoodie.read.blob.inline.mode", "CONTENT")`
   * on `spark.read.format("hudi")`, which is the typical way users disable this check
   * for a single query.
   *
   * Returns true (DESCRIPTOR) when no relation can be located — i.e., the rule prefers
   * to fail noisily for an unrecognized relation shape rather than silently let a
   * mixed projection through. The error message tells the user how to opt out.
   */
  private def effectiveModeIsDescriptor(plan: LogicalPlan): Boolean = {
    val modeKey = HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()
    val maybeLr: Option[LogicalRelation] = plan.collectFirst { case lr: LogicalRelation => lr }
    val relationOpt: Option[String] = maybeLr.flatMap { lr =>
      lr.relation match {
        case hfs: HadoopFsRelation => hfs.options.get(modeKey)
        case _ => None
      }
    }
    val tableProps: Map[String, String] = maybeLr
      .flatMap(_.catalogTable)
      .map(_.properties.toMap)
      .getOrElse(Map.empty)
    val effective = relationOpt
      .orElse(spark.conf.getOption(modeKey))
      .orElse(tableProps.get(modeKey))
      .getOrElse(HoodieReaderConfig.BLOB_INLINE_READ_MODE.defaultValue())
    effective.equalsIgnoreCase(HoodieReaderConfig.BLOB_INLINE_READ_MODE_DESCRIPTOR)
  }
}
