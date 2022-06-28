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

package org.apache.spark.sql.parser

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.spark.sql.parser.HoodieSqlCommonBaseVisitor
import org.apache.hudi.spark.sql.parser.HoodieSqlCommonParser._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical._

import java.util.Locale
import scala.collection.JavaConverters._

class HoodieSqlCommonAstBuilder(session: SparkSession, delegate: ParserInterface)
  extends HoodieSqlCommonBaseVisitor[AnyRef] with Logging with SparkAdapterSupport {

  import ParserUtils._

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    ctx.statement().accept(this).asInstanceOf[LogicalPlan]
  }

  override def visitCompactionOnTable(ctx: CompactionOnTableContext): LogicalPlan = withOrigin(ctx) {
    val table = ctx.tableIdentifier().accept(this).asInstanceOf[LogicalPlan]
    val operation = CompactionOperation.withName(ctx.operation.getText.toUpperCase)
    val timestamp = if (ctx.instantTimestamp != null) Some(ctx.instantTimestamp.getText.toLong) else None
    CompactionTable(table, operation, timestamp)
  }

  override def visitCompactionOnPath(ctx: CompactionOnPathContext): LogicalPlan = withOrigin(ctx) {
    val path = string(ctx.path)
    val operation = CompactionOperation.withName(ctx.operation.getText.toUpperCase)
    val timestamp = if (ctx.instantTimestamp != null) Some(ctx.instantTimestamp.getText.toLong) else None
    CompactionPath(path, operation, timestamp)
  }

  override def visitShowCompactionOnTable(ctx: ShowCompactionOnTableContext): LogicalPlan = withOrigin(ctx) {
    val table = ctx.tableIdentifier().accept(this).asInstanceOf[LogicalPlan]
    if (ctx.limit != null) {
      CompactionShowOnTable(table, ctx.limit.getText.toInt)
    } else {
      CompactionShowOnTable(table)
    }
  }

  override def visitShowCompactionOnPath(ctx: ShowCompactionOnPathContext): LogicalPlan = withOrigin(ctx) {
    val path = string(ctx.path)
    if (ctx.limit != null) {
      CompactionShowOnPath(path, ctx.limit.getText.toInt)
    } else {
      CompactionShowOnPath(path)
    }
  }

  override def visitTableIdentifier(ctx: TableIdentifierContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText)))
  }

  override def visitCall(ctx: CallContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.callArgument().isEmpty) {
      throw new ParseException(s"Procedures arguments is empty", ctx)
    }

    val name: Seq[String] = ctx.multipartIdentifier().parts.asScala.map(_.getText)
    val args: Seq[CallArgument] = ctx.callArgument().asScala.map(typedVisit[CallArgument])
    CallCommand(name, args)
  }

  /**
   * Return a multi-part identifier as Seq[String].
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    ctx.parts.asScala.map(_.getText)
  }

  /**
   * Create a positional argument in a stored procedure call.
   */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): CallArgument = withOrigin(ctx) {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }

  /**
   * Create a named argument in a stored procedure call.
   */
  override def visitNamedArgument(ctx: NamedArgumentContext): CallArgument = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val expr = typedVisit[Expression](ctx.expression)
    NamedArgument(name, expr)
  }

  def visitConstant(ctx: ConstantContext): Literal = {
    delegate.parseExpression(ctx.getText).asInstanceOf[Literal]
  }

  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala.map {
      case c: ParserRuleContext => reconstructSqlString(c)
      case t: TerminalNode => t.getText
    }.mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  /**
   * Create an index, returning a [[CreateIndex]] logical plan.
   * For example:
   * {{{
   * CREATE INDEX index_name ON [TABLE] table_name [USING index_type] (column_index_property_list)
   *   [OPTIONS indexPropertyList]
   *   column_index_property_list: column_name [OPTIONS(indexPropertyList)]  [ ,  . . . ]
   *   indexPropertyList: index_property_name [= index_property_value] [ ,  . . . ]
   * }}}
   */
  override def visitCreateIndex(ctx: CreateIndexContext): LogicalPlan = withOrigin(ctx) {
    val (indexName, indexType) = if (ctx.identifier.size() == 1) {
      (ctx.identifier(0).getText, "")
    } else {
      (ctx.identifier(0).getText, ctx.identifier(1).getText)
    }

    val columns = ctx.columns.multipartIdentifierProperty.asScala
        .map(_.multipartIdentifier).map(typedVisit[Seq[String]]).toSeq
    val columnsProperties = ctx.columns.multipartIdentifierProperty.asScala
        .map(x => (Option(x.options).map(visitPropertyKeyValues).getOrElse(Map.empty))).toSeq
    val options = Option(ctx.indexOptions).map(visitPropertyKeyValues).getOrElse(Map.empty)

    CreateIndex(
      visitTableIdentifier(ctx.tableIdentifier()),
      indexName,
      indexType,
      ctx.EXISTS != null,
      columns.map(UnresolvedAttribute(_)).zip(columnsProperties),
      options)
  }

  /**
   * Drop an index, returning a [[DropIndex]] logical plan.
   * For example:
   * {{{
   *   DROP INDEX [IF EXISTS] index_name ON [TABLE] table_name
   * }}}
   */
  override def visitDropIndex(ctx: DropIndexContext): LogicalPlan = withOrigin(ctx) {
    val indexName = ctx.identifier.getText
    DropIndex(
      visitTableIdentifier(ctx.tableIdentifier()),
      indexName,
      ctx.EXISTS != null)
  }

  /**
   * Show indexes, returning a [[ShowIndexes]] logical plan.
   * For example:
   * {{{
   *   SHOW INDEXES (FROM | IN) [TABLE] table_name
   * }}}
   */
  override def visitShowIndexes(ctx: ShowIndexesContext): LogicalPlan = withOrigin(ctx) {
    ShowIndexes(visitTableIdentifier(ctx.tableIdentifier()))
  }

  /**
   * Refresh index, returning a [[RefreshIndex]] logical plan
   * For example:
   * {{{
   *   REFRESH INDEX index_name ON [TABLE] table_name
   * }}}
   */
  override def visitRefreshIndex(ctx: RefreshIndexContext): LogicalPlan = withOrigin(ctx) {
    RefreshIndex(visitTableIdentifier(ctx.tableIdentifier()), ctx.identifier.getText)
  }

  /**
   * Convert a property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitPropertyList(
      ctx: PropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[PropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: PropertyListContext): Map[String, String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[PropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: PropertyListContext): Seq[String] = {
    val props = visitPropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a property
   * identifier.
   */
  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * A property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }
}
