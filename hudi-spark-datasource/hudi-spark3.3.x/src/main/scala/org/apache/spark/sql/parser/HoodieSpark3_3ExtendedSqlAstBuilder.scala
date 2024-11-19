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

import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseParser._
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseBaseVisitor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.parser.ParserUtils.{checkDuplicateKeys, operationNotAllowed, string, withOrigin}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

import java.util.Locale
import scala.collection.JavaConverters._

/**
 * The AstBuilder for HoodieSqlParser to parser the AST tree to Logical Plan.
 * Here we only do the parser for the extended sql syntax. e.g Create Index. For
 * other sql syntax we use the delegate sql parser which is the SparkSqlParser.
 */
class HoodieSpark3_3ExtendedSqlAstBuilder()
  extends HoodieSqlBaseBaseVisitor[AnyRef] with Logging {

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

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

  // ============== The following code is fork from org.apache.spark.sql.catalyst.parser.AstBuilder
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */
  /**
   * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
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
      UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())),
      indexName,
      indexType,
      ctx.EXISTS != null,
      columns.map(UnresolvedFieldName).zip(columnsProperties),
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
      UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())),
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
    ShowIndexes(UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())))
  }

  /**
   * Refresh index, returning a [[RefreshIndex]] logical plan
   * For example:
   * {{{
   *   REFRESH INDEX index_name ON [TABLE] table_name
   * }}}
   */
  override def visitRefreshIndex(ctx: RefreshIndexContext): LogicalPlan = withOrigin(ctx) {
    RefreshIndex(UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier())), ctx.identifier.getText)
  }

  /**
   * Convert a property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitPropertyList(ctx: PropertyListContext): Map[String, String] = withOrigin(ctx) {
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
