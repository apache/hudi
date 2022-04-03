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
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical._

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
}
