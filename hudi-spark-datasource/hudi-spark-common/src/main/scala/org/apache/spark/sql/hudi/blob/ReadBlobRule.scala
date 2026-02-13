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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

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
    case Project(projectList, child)
      if containsReadBlobExpression(projectList)
        && !child.isInstanceOf[BatchedBlobRead] =>

      val blobColumn =
        extractBlobColumnFromExpressions(projectList).getOrElse {
          throw new IllegalStateException(
            "read_blob() function found but no valid blob column reference could be extracted. " +
              "Ensure that read_blob() is called on a BLOB type."
          )
        }

      val wrapped = BatchedBlobRead(child, blobColumn)

      val newProjectList =
        transformNamedExpressions(projectList, wrapped.dataAttr)

      Project(newProjectList, wrapped)
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

  private def transformNamedExpressions(expressions: Seq[NamedExpression],
                                         dataAttr: Attribute): Seq[NamedExpression] = {

    expressions.map {

      case alias @ Alias(childExpr, name) =>
        val rewritten = replaceReadBlobExpression(childExpr, dataAttr)
        Alias(rewritten, name)(
          alias.exprId,
          alias.qualifier,
          alias.explicitMetadata
        )

      case attr: AttributeReference =>
        // No remapping needed anymore
        attr

      case other =>
        replaceReadBlobExpression(other, dataAttr)
          .asInstanceOf[NamedExpression]
    }
  }

  /**
   * Replace ReadBlobExpression with reference to resolved data column.
   *
   * Recursively traverses expression tree and replaces all [[ReadBlobExpression]]
   * nodes with references to the data column produced by [[BatchedByteRangeReader]].
   */
  private def replaceReadBlobExpression(
      expr: Expression,
      dataAttr: Attribute): Expression = expr match {

    case ReadBlobExpression(_) =>
      // Replace with reference to the data column
      dataAttr

    case other =>
      // Recursively process children
      other.mapChildren(child => replaceReadBlobExpression(child, dataAttr))
  }

  /**
   * Extract blob column name from expressions (handles nesting in aggregate functions).
   */
  private def extractBlobColumnFromExpressions(expressions: Seq[Expression]): Option[AttributeReference] = {
    expressions.collectFirst {
      case expr if containsReadBlobInExpression(expr) =>
        findBlobColumn(expr)
    }.flatten
  }

  /**
   * Recursively search for ReadBlobExpression and extract column name.
   */
  private def findBlobColumn(expr: Expression): Option[AttributeReference] = expr match {
    case ReadBlobExpression(child) =>
      child match {
        case attr: AttributeReference => Some(attr)
        case _ => None
      }
    case other =>
      other.children.flatMap(findBlobColumn).headOption
  }
}
