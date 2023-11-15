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

package org.apache.spark.sql

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeEq, AttributeReference, AttributeSet, Cast, Expression, Like, Literal, SubqueryExpression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

trait HoodieCatalystExpressionUtils {

  /**
   * SPARK-44531 Encoder inference moved elsewhere in Spark 3.5.0
   * Mainly used for unit tests
   */
  def getEncoder(schema: StructType): ExpressionEncoder[Row]
  
  /**
   * Returns a filter that its reference is a subset of `outputSet` and it contains the maximum
   * constraints from `condition`. This is used for predicate push-down
   * When there is no such filter, `None` is returned.
   */
  def extractPredicatesWithinOutputSet(condition: Expression,
                                       outputSet: AttributeSet): Option[Expression]

  /**
   * The attribute name may differ from the one in the schema if the query analyzer
   * is case insensitive. We should change attribute names to match the ones in the schema,
   * so we do not need to worry about case sensitivity anymore
   */
  def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression]

  // TODO scala-doc
  def matchCast(expr: Expression): Option[(Expression, DataType, Option[String])]

  /**
   * Matches an expression iff
   *
   * <ol>
   *   <li>It references exactly one [[AttributeReference]]</li>
   *   <li>It contains only whitelisted transformations that preserve ordering of the source column [1]</li>
   * </ol>
   *
   * [1] Preserving ordering is defined as following: transformation T is defined as ordering preserving in case
   *     values of the source column A values being ordered as a1, a2, a3 ..., will map into column B = T(A) which
   *     will keep the same ordering b1, b2, b3, ... with b1 = T(a1), b2 = T(a2), ...
   */
  def tryMatchAttributeOrderingPreservingTransformation(expr: Expression): Option[AttributeReference]

  /**
   * Verifies whether [[fromType]] can be up-casted to [[toType]]
   */
  def canUpCast(fromType: DataType, toType: DataType): Boolean

  /**
   * Un-applies [[Cast]] expression into
   * <ol>
   *   <li>Casted [[Expression]]</li>
   *   <li>Target [[DataType]]</li>
   *   <li>(Optional) Timezone spec</li>
   *   <li>Flag whether it's an ANSI cast or not</li>
   * </ol>
   */
  def unapplyCastExpression(expr: Expression): Option[(Expression, DataType, Option[String], Boolean)]
}

object HoodieCatalystExpressionUtils extends SparkAdapterSupport {

  /**
   * Convenience extractor allowing to untuple [[Cast]] across Spark versions
   */
  object MatchCast {
    def unapply(expr: Expression): Option[(Expression, DataType, Option[String], Boolean)] =
      sparkAdapter.getCatalystExpressionUtils.unapplyCastExpression(expr)
  }

  /**
   * Leverages [[AttributeEquals]] predicate on 2 provided [[Attribute]]s
   */
  def attributeEquals(one: Attribute, other: Attribute): Boolean =
    new AttributeEq(one).equals(new AttributeEq(other))

  /**
   * Generates instance of [[UnsafeProjection]] projecting row of one [[StructType]] into another [[StructType]]
   *
   * NOTE: No safety checks are executed to validate that this projection is actually feasible,
   * it's up to the caller to make sure that such projection is possible.
   *
   * NOTE: Projection of the row from [[StructType]] A to [[StructType]] B is only possible, if
   * B is a subset of A
   */
  def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection = {
    val projection = generateUnsafeProjectionInternal(from, to)
    val identical = from == to
    // NOTE: Have to use explicit [[Projection]] instantiation to stay compatible w/ Scala 2.11
    new UnsafeProjection {
      override def apply(row: InternalRow): UnsafeRow =
        row match {
          case ur: UnsafeRow if identical => ur
          case _ => projection(row)
        }
    }
  }

  /**
   * Split the given predicates into two sequence predicates:
   * - predicates that references partition columns only(and involves no sub-query);
   * - other predicates.
   *
   * @param sparkSession     The spark session
   * @param predicates       The predicates to be split
   * @param partitionColumns The partition columns
   * @return (partitionFilters, dataFilters)
   */
  def splitPartitionAndDataPredicates(sparkSession: SparkSession,
                                      predicates: Array[Expression],
                                      partitionColumns: Array[String]): (Array[Expression], Array[Expression]) = {
    // Validates that the provided names both resolve to the same entity
    val resolvedNameEquals = sparkSession.sessionState.analyzer.resolver

    predicates.partition(expr => {
      // Checks whether given expression only references partition columns(and involves no sub-query)
      expr.references.forall(r => partitionColumns.exists(resolvedNameEquals(r.name, _))) &&
        !SubqueryExpression.hasSubquery(expr)
    })
  }

  /**
   * Parses and resolves expression against the attributes of the given table schema.
   *
   * For example:
   * <pre>
   * ts > 1000 and ts <= 1500
   * </pre>
   * will be resolved as
   * <pre>
   * And(GreaterThan(ts#590L > 1000), LessThanOrEqual(ts#590L <= 1500))
   * </pre>
   *
   * Where <pre>ts</pre> is a column of the provided [[tableSchema]]
   *
   * @param spark       spark session
   * @param exprString  string representation of the expression to parse and resolve
   * @param tableSchema table schema encompassing attributes to resolve against
   * @return Resolved filter expression
   */
  def resolveExpr(spark: SparkSession, exprString: String, tableSchema: StructType): Expression = {
    val expr = spark.sessionState.sqlParser.parseExpression(exprString)
    resolveExpr(spark, expr, tableSchema)
  }

  /**
   * Resolves provided expression (unless already resolved) against the attributes of the given table schema.
   *
   * For example:
   * <pre>
   * ts > 1000 and ts <= 1500
   * </pre>
   * will be resolved as
   * <pre>
   * And(GreaterThan(ts#590L > 1000), LessThanOrEqual(ts#590L <= 1500))
   * </pre>
   *
   * Where <pre>ts</pre> is a column of the provided [[tableSchema]]
   *
   * @param spark       spark session
   * @param expr        Catalyst expression to be resolved (if not yet)
   * @param tableSchema table schema encompassing attributes to resolve against
   * @return Resolved filter expression
   */
  def resolveExpr(spark: SparkSession, expr: Expression, tableSchema: StructType): Expression = {
    val analyzer = spark.sessionState.analyzer
    val schemaFields = tableSchema.fields

    import org.apache.spark.sql.catalyst.plans.logical.Filter
    val resolvedExpr = {
      val plan: LogicalPlan = Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
      analyzer.execute(plan).asInstanceOf[Filter].condition
    }

    if (!hasUnresolvedRefs(resolvedExpr)) {
      resolvedExpr
    } else {
      throw new IllegalStateException("unresolved attribute")
    }
  }

  /**
   * Converts [[Filter]] to Catalyst [[Expression]]
   */
  def convertToCatalystExpression(filter: Filter, tableSchema: StructType): Option[Expression] = {
    Option(
      filter match {
        case EqualTo(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.EqualTo(toAttribute(attribute, tableSchema), Literal.create(value))
        case EqualNullSafe(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.EqualNullSafe(toAttribute(attribute, tableSchema), Literal.create(value))
        case GreaterThan(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.GreaterThan(toAttribute(attribute, tableSchema), Literal.create(value))
        case GreaterThanOrEqual(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(toAttribute(attribute, tableSchema), Literal.create(value))
        case LessThan(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.LessThan(toAttribute(attribute, tableSchema), Literal.create(value))
        case LessThanOrEqual(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(toAttribute(attribute, tableSchema), Literal.create(value))
        case In(attribute, values) =>
          val attrExp = toAttribute(attribute, tableSchema)
          val valuesExp = values.map(v => Literal.create(v))
          org.apache.spark.sql.catalyst.expressions.In(attrExp, valuesExp)
        case IsNull(attribute) =>
          org.apache.spark.sql.catalyst.expressions.IsNull(toAttribute(attribute, tableSchema))
        case IsNotNull(attribute) =>
          org.apache.spark.sql.catalyst.expressions.IsNotNull(toAttribute(attribute, tableSchema))
        case And(left, right) =>
          val leftExp = convertToCatalystExpression(left, tableSchema)
          val rightExp = convertToCatalystExpression(right, tableSchema)
          if (leftExp.isEmpty || rightExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.And(leftExp.get, rightExp.get)
          }
        case Or(left, right) =>
          val leftExp = convertToCatalystExpression(left, tableSchema)
          val rightExp = convertToCatalystExpression(right, tableSchema)
          if (leftExp.isEmpty || rightExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.Or(leftExp.get, rightExp.get)
          }
        case Not(child) =>
          val childExp = convertToCatalystExpression(child, tableSchema)
          if (childExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.Not(childExp.get)
          }
        case StringStartsWith(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"$value%")
          new Like(leftExp, rightExp)
        case StringEndsWith(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"%$value")
          new Like(leftExp, rightExp)
        case StringContains(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"%$value%")
          new Like(leftExp, rightExp)
        case _ => null
      }
    )
  }

  private def generateUnsafeProjectionInternal(from: StructType, to: StructType): UnsafeProjection = {
    val attrs = sparkAdapter.getSchemaUtils.toAttributes(from)
    val attrsMap = attrs.map(attr => (attr.name, attr)).toMap
    val targetExprs = to.fields.map(f => attrsMap(f.name))

    UnsafeProjection.create(targetExprs, attrs)
  }

  private def hasUnresolvedRefs(resolvedExpr: Expression): Boolean =
    resolvedExpr.collectFirst {
      case _: UnresolvedAttribute | _: UnresolvedFunction => true
    }.isDefined

  private def toAttribute(columnName: String, tableSchema: StructType): AttributeReference = {
    val field = tableSchema.find(p => p.name == columnName)
    assert(field.isDefined, s"Cannot find column: $columnName, Table Columns are: " +
      s"${tableSchema.fieldNames.mkString(",")}")
    AttributeReference(columnName, field.get.dataType, field.get.nullable)()
  }
}
