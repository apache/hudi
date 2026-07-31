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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
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
 * Supports both top-level column references and nested expressions that
 * resolve to a BlobType struct (e.g. `read_blob(parent.payload)` or
 * `read_blob(arr[0])`). Non-attribute sources are lifted to a synthetic
 * top-level alias via an injected `Project` so the downstream
 * [[BatchedBlobRead]] always sees a plain top-level attribute.
 *
 * Example: `SELECT id, read_blob(image_data) FROM table`
 * Example: `SELECT id, read_blob(doc.payload) FROM table`
 *
 * @param spark SparkSession for accessing configuration
 */
case class ReadBlobRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case Project(projectList, Filter(condition, child))
      if containsReadBlobExpression(projectList)
        && containsReadBlobInExpression(condition)
        && !child.isInstanceOf[BatchedBlobRead] =>
      val sources = dedupBlobSources(
        extractAllBlobSourceExprs(projectList) ++ extractBlobSourceExprsFromExpression(condition))
      val (wrappedPlan, sourceToDataAttr) = wrapWithBlobReads(sources, child)
      val newCondition = replaceReadBlobExpression(condition, sourceToDataAttr)
      val newProjectList = transformNamedExpressions(projectList, sourceToDataAttr)
      Project(newProjectList, Filter(newCondition, wrappedPlan))

    case Filter(condition, child)
      if containsReadBlobInExpression(condition)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val sources = extractBlobSourceExprsFromExpression(condition)
      val (wrappedPlan, sourceToDataAttr) = wrapWithBlobReads(sources, child)
      val newCondition = replaceReadBlobExpression(condition, sourceToDataAttr)
      Project(child.output, Filter(newCondition, wrappedPlan))

    case Project(projectList, child)
      if containsReadBlobExpression(projectList)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val sources = extractAllBlobSourceExprs(projectList)
      val (wrappedPlan, sourceToDataAttr) = wrapWithBlobReads(sources, child)
      val newProjectList = transformNamedExpressions(projectList, sourceToDataAttr)
      Project(newProjectList, wrappedPlan)

    case node if containsReadBlobInAnyExpression(node) =>
      throw new IllegalArgumentException(
        s"read_blob() may only appear in SELECT or WHERE clauses. Found in unsupported logical plan node: ${node.nodeName}. " +
        s"Move read_blob() to a SELECT or WHERE clause. Full plan: ${node.simpleStringWithNodeId()}")
  }

  private def containsReadBlobInAnyExpression(plan: LogicalPlan): Boolean = {
    plan.expressions.exists(containsReadBlobInExpression)
  }

  /**
   * Wraps `child` with one [[BatchedBlobRead]] per distinct blob source expression.
   * Non-attribute sources are first projected to a synthetic top-level alias so the
   * downstream nodes can address them as plain top-level attributes.
   *
   * @return the augmented plan plus a mapping from each source expression's
   *         canonicalized form to the [[BatchedBlobRead.dataAttr]] that should
   *         replace the corresponding `read_blob()` call.
   */
  private def wrapWithBlobReads(
      sources: Seq[Expression],
      child: LogicalPlan): (LogicalPlan, Map[Expression, Attribute]) = {
    if (sources.isEmpty) {
      throw new IllegalStateException("read_blob() found but no valid blob column reference extracted.")
    }

    // Per source: either reuse an existing top-level AttributeReference, or synthesize a
    // fresh Alias that we will project onto the plan to lift the nested expression into a
    // top-level attribute. We keep the original source expression so we can key the final
    // mapping by its canonicalized form.
    case class Binding(source: Expression, blobAttr: AttributeReference, lift: Option[Alias])
    val bindings: Seq[Binding] = sources.map {
      case attr: AttributeReference =>
        Binding(attr, attr, None)
      case other =>
        // Deterministic alias name from the canonicalized form so identical nested
        // sub-trees (e.g. across SELECT and WHERE) get the same alias name.
        val aliasName = s"_blob_src_${Integer.toHexString(other.canonicalized.hashCode())}"
        val alias = Alias(other, aliasName)()
        Binding(other, alias.toAttribute.asInstanceOf[AttributeReference], Some(alias))
    }

    // Type-compatibility check: every blob source must resolve to a BlobType struct.
    bindings.foreach { b =>
      b.blobAttr.dataType match {
        case struct: StructType
          if DataType.equalsIgnoreCaseAndNullability(struct, org.apache.spark.sql.types.BlobType.dataType) =>
          // Valid blob source.
        case _ =>
          throw new IllegalArgumentException(
            s"Blob source '${b.source}' must be compatible with BlobType (type, data, reference struct), " +
              s"found: ${b.blobAttr.dataType}")
      }
    }

    // Inject the synthetic Project once if any source needed lifting.
    val lifts = bindings.flatMap(_.lift)
    val planWithLifts: LogicalPlan = if (lifts.isEmpty) child
      else Project(child.output ++ lifts, child)

    // Stack one BatchedBlobRead per distinct blob source.
    val (finalPlan, mapping) = bindings.foldLeft(
      (planWithLifts, Map.empty[Expression, Attribute])) {
      case ((currentPlan, acc), b) =>
        val blobRead = BatchedBlobRead(currentPlan, b.blobAttr)
        (blobRead, acc + (b.source.canonicalized -> blobRead.dataAttr))
    }

    (finalPlan, mapping)
  }

  /**
   * Check if any expression in the project list contains a ReadBlobExpression.
   */
  private def containsReadBlobExpression(projectList: Seq[Expression]): Boolean = {
    projectList.exists(containsReadBlobInExpression)
  }

  private def containsReadBlobInExpression(expr: Expression): Boolean = {
    expr match {
      case _: ReadBlobExpression => true
      case other => other.children.exists(containsReadBlobInExpression)
    }
  }

  private def extractAllBlobSourceExprs(expressions: Seq[Expression]): Seq[Expression] = {
    val seen = mutable.LinkedHashSet.empty[Expression]
    val result = ArrayBuffer.empty[Expression]
    expressions.foreach(collectBlobSources(_, seen, result))
    result.toSeq
  }

  private def extractBlobSourceExprsFromExpression(expr: Expression): Seq[Expression] = {
    val seen = mutable.LinkedHashSet.empty[Expression]
    val result = ArrayBuffer.empty[Expression]
    collectBlobSources(expr, seen, result)
    result.toSeq
  }

  /**
   * Walks `expr` collecting the inner expression of every `ReadBlobExpression`.
   * Dedup is by canonicalized form so identical sub-trees only produce one
   * [[BatchedBlobRead]] each.
   */
  private def collectBlobSources(
      expr: Expression,
      seen: mutable.Set[Expression],
      result: ArrayBuffer[Expression]): Unit = expr match {
    case ReadBlobExpression(inner) =>
      if (seen.add(inner.canonicalized)) result += inner
    case other =>
      other.children.foreach(collectBlobSources(_, seen, result))
  }

  /**
   * Dedup two source-expression lists (e.g. from a Project + Filter) by canonicalized
   * form, preserving first-seen order.
   */
  private def dedupBlobSources(sources: Seq[Expression]): Seq[Expression] = {
    val seen = mutable.LinkedHashSet.empty[Expression]
    val result = ArrayBuffer.empty[Expression]
    sources.foreach { s =>
      if (seen.add(s.canonicalized)) result += s
    }
    result.toSeq
  }

  private def transformNamedExpressions(
      expressions: Seq[NamedExpression],
      sourceToDataAttr: Map[Expression, Attribute]): Seq[NamedExpression] = {
    expressions.map {
      case alias @ Alias(childExpr, name) =>
        val rewritten = replaceReadBlobExpression(childExpr, sourceToDataAttr)
        Alias(rewritten, name)(alias.exprId, alias.qualifier, alias.explicitMetadata)
      case attr: AttributeReference => attr
      case other =>
        replaceReadBlobExpression(other, sourceToDataAttr).asInstanceOf[NamedExpression]
    }
  }

  private def replaceReadBlobExpression(
      expr: Expression,
      sourceToDataAttr: Map[Expression, Attribute]): Expression = expr match {
    case ReadBlobExpression(inner) =>
      sourceToDataAttr.getOrElse(inner.canonicalized, throw new IllegalArgumentException(
        s"read_blob() called on expression '$inner' which was not registered for blob reading. " +
        s"Available blob sources: ${sourceToDataAttr.keys.mkString(", ")}"))
    case other =>
      other.mapChildren(replaceReadBlobExpression(_, sourceToDataAttr))
  }
}
