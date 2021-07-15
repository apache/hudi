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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.spark.sql.parser.HoodieSqlCommonBaseVisitor
import org.apache.hudi.spark.sql.parser.HoodieSqlCommonParser.{ClusteringOnPathContext, ClusteringOnTableContext, CompactionOnPathContext, CompactionOnTableContext, ShowClusteringOnPathContext, ShowClusteringOnTableContext, ShowCompactionOnPathContext, ShowCompactionOnTableContext, SingleStatementContext, TableIdentifierContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.{ClusteringOnPath, ClusteringOnTable, CompactionOperation, CompactionPath, CompactionShowOnPath, CompactionShowOnTable, CompactionTable, LogicalPlan, ShowClusteringOnPath, ShowClusteringOnTable}

import scala.collection.JavaConverters._

class HoodieSqlCommonAstBuilder(session: SparkSession, delegate: ParserInterface)
  extends HoodieSqlCommonBaseVisitor[AnyRef] with Logging with SparkAdapterSupport {

  import ParserUtils._

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    ctx.statement().accept(this).asInstanceOf[LogicalPlan]
  }

  override def visitCompactionOnTable(ctx: CompactionOnTableContext): LogicalPlan = withOrigin(ctx) {
    val table = ctx.tableIdentifier().accept(this).asInstanceOf[LogicalPlan]
    val operation = CompactionOperation.withName(ctx.operation.getText.toUpperCase)
    val timestamp = if (ctx.instantTimestamp != null) Some(ctx.instantTimestamp.getText.toLong) else None
    CompactionTable(table, operation, timestamp)
  }

  override def visitCompactionOnPath (ctx: CompactionOnPathContext): LogicalPlan = withOrigin(ctx) {
    val path = string(ctx.path)
    val operation = CompactionOperation.withName(ctx.operation.getText.toUpperCase)
    val timestamp = if (ctx.instantTimestamp != null) Some(ctx.instantTimestamp.getText.toLong) else None
    CompactionPath(path, operation, timestamp)
  }

  override def visitShowCompactionOnTable (ctx: ShowCompactionOnTableContext): LogicalPlan = withOrigin(ctx) {
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

  override def visitClusteringOnTable(ctx: ClusteringOnTableContext): LogicalPlan = withOrigin(ctx) {
    val table = ctx.tableIdentifier().accept(this).asInstanceOf[LogicalPlan]
    val orderByColumns = if (ctx.orderColumn != null) {
      ctx.orderColumn.asScala.map(_.getText)
    } else {
      Seq.empty[String]
    }
    val timestamp = if (ctx.timestamp != null) Some(ctx.timestamp.getText.toLong) else None
    ClusteringOnTable(table, orderByColumns, timestamp)
  }

  override def visitClusteringOnPath(ctx: ClusteringOnPathContext): LogicalPlan = withOrigin(ctx) {
    val path = string(ctx.path)
    val orderByColumns = if (ctx.orderColumn != null) {
      ctx.orderColumn.asScala.map(_.getText)
    } else {
      Seq.empty[String]
    }
    val timestamp = if (ctx.timestamp != null) Some(ctx.timestamp.getText.toLong) else None
    ClusteringOnPath(path, orderByColumns, timestamp)
  }

  override def visitShowClusteringOnTable(ctx: ShowClusteringOnTableContext): LogicalPlan = withOrigin(ctx) {
    val table = ctx.tableIdentifier().accept(this).asInstanceOf[LogicalPlan]
    if (ctx.limit != null) {
      ShowClusteringOnTable(table, ctx.limit.getText.toInt)
    } else {
      ShowClusteringOnTable(table)
    }
  }

  override def visitShowClusteringOnPath(ctx: ShowClusteringOnPathContext): LogicalPlan = withOrigin(ctx) {
    val path = string(ctx.path)
    if (ctx.limit != null) {
      ShowClusteringOnPath(path, ctx.limit.getText.toInt)
    } else {
      ShowClusteringOnPath(path)
    }
  }

  override def visitTableIdentifier(ctx: TableIdentifierContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText)))
  }
}
