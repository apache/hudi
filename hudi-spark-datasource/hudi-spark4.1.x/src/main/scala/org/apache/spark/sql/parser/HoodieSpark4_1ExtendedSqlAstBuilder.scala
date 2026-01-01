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

import org.apache.hudi.spark.sql.parser.{HoodieSqlBaseBaseVisitor, HoodieSqlBaseParser}
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseParser._

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.parser.{EnhancedLogicalPlan, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParserUtils.{checkDuplicateClauses, checkDuplicateKeys, entry, escapedIdentifier, operationNotAllowed, source, string, stringWithoutUnescape, validate, withOrigin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils, DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.BucketSpecHelper
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
import org.apache.spark.sql.connector.expressions.{ApplyTransform, BucketTransform, DaysTransform, Expression => V2Expression, FieldReference, HoursTransform, IdentityTransform, LiteralValue, MonthsTransform, Transform, YearsTransform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.Utils.isTesting
import org.apache.spark.util.random.RandomSampler

import javax.xml.bind.DatatypeConverter

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * The AstBuilder for HoodieSqlParser to parser the AST tree to Logical Plan.
 * Here we only do the parser for the extended sql syntax. e.g MergeInto. For
 * other sql syntax we use the delegate sql parser which is the SparkSqlParser.
 */
class HoodieSpark4_1ExtendedSqlAstBuilder(conf: SQLConf, delegate: ParserInterface)
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

  /**
   * Create an aliased table reference. This is typically used in FROM clauses.
   */
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val tableId = visitMultipartIdentifier(ctx.multipartIdentifier())
    val relation = UnresolvedRelation(tableId)
    val table = mayApplyAliasPlan(
      ctx.tableAlias, relation.optionalMap(ctx.temporalClause)(withTimeTravel))
    table.optionalMap(ctx.sample)(withSample)
  }

  private def withTimeTravel(
                              ctx: TemporalClauseContext, plan: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val v = ctx.version
    val version = if (ctx.INTEGER_VALUE != null) {
      Some(v.getText)
    } else {
      Option(v).map(string)
    }

    val timestamp = Option(ctx.timestamp).map(expression)
    if (timestamp.exists(_.references.nonEmpty)) {
      throw new ParseException(
        "timestamp expression cannot refer to any columns", ctx.timestamp)
    }
    if (timestamp.exists(e => SubqueryExpression.hasSubquery(e))) {
      throw new ParseException(
        "timestamp expression cannot contain subqueries", ctx.timestamp)
    }

    TimeTravelRelation(plan, timestamp, version)
  }

  // ============== The following code is fork from org.apache.spark.sql.catalyst.parser.AstBuilder
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleTableIdentifier(
                                           ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    visitTableIdentifier(ctx.tableIdentifier)
  }

  override def visitSingleFunctionIdentifier(
                                              ctx: SingleFunctionIdentifierContext): FunctionIdentifier = withOrigin(ctx) {
    visitFunctionIdentifier(ctx.functionIdentifier)
  }

  override def visitSingleMultipartIdentifier(
                                               ctx: SingleMultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    visitMultipartIdentifier(ctx.multipartIdentifier)
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    typedVisit[DataType](ctx.dataType)
  }

  override def visitSingleTableSchema(ctx: SingleTableSchemaContext): StructType = {
    val schema = StructType(visitColTypeList(ctx.colTypeList))
    withOrigin(ctx)(schema)
  }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create a top-level plan with Common Table Expressions.
   */
  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    val query = plan(ctx.queryTerm).optionalMap(ctx.queryOrganization)(withQueryResultClauses)

    // Apply CTEs
    query.optionalMap(ctx.ctes)(withCTE)
  }

  override def visitDmlStatement(ctx: DmlStatementContext): AnyRef = withOrigin(ctx) {
    val dmlStmt = plan(ctx.dmlStatementNoWith)
    // Apply CTEs
    dmlStmt.optionalMap(ctx.ctes)(withCTE)
  }

  private def withCTE(ctx: CtesContext, plan: LogicalPlan): LogicalPlan = {
    val ctes = ctx.namedQuery.asScala.map { nCtx =>
      val namedQuery = visitNamedQuery(nCtx)
      val rowLevelLimit: Option[Int] = if (nCtx.integerValue() != null) {
        if (ctx.RECURSIVE() == null) {
          operationNotAllowed("Cannot specify MAX RECURSION LEVEL when the CTE is not marked as " +
            "RECURSIVE", ctx)
        }
        Some(getIntegerValue(nCtx.integerValue()))
      } else {
        None
      }
      (namedQuery.alias, namedQuery, rowLevelLimit)
    }
    // Check for duplicate names.
    val duplicates = ctes.groupBy(_._1).filter(_._2.size > 1).keys
    if (duplicates.nonEmpty) {
      throw new ParseException(s"CTE definition can't have duplicate names: ${duplicates.mkString("'", "', '", "'")}.", ctx)
    }
    UnresolvedWith(plan, ctes.toSeq, ctx.RECURSIVE() != null)
  }

  /**
   * Gets the integer value from an IntegerValueContext after parameter replacement. Asserts that
   * parameter markers have been substituted before reaching DataTypeAstBuilder.
   *
   * @param ctx
   * The IntegerValueContext to extract the integer from
   * @return
   * The integer value
   */
  private def getIntegerValue(ctx: IntegerValueContext): Int = {
    assert(
      !ctx.isInstanceOf[ParameterIntegerValueContext],
      "Parameter markers should be substituted before DataTypeAstBuilder processes the " +
        s"parse tree. Found unsubstituted parameter: ${ctx.getText}")
    ctx.getText.toInt
  }

  /**
   * Create a logical query plan for a hive-style FROM statement body.
   */
  private def withFromStatementBody(
                                     ctx: FromStatementBodyContext, plan: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // two cases for transforms and selects
    if (ctx.transformClause != null) {
      withTransformQuerySpecification(
        ctx,
        ctx.transformClause,
        ctx.lateralView,
        ctx.whereClause,
        ctx.aggregationClause,
        ctx.havingClause,
        ctx.windowClause,
        plan
      )
    } else {
      withSelectQuerySpecification(
        ctx,
        ctx.selectClause,
        ctx.lateralView,
        ctx.whereClause,
        ctx.aggregationClause,
        ctx.havingClause,
        ctx.windowClause,
        plan
      )
    }
  }

  override def visitFromStatement(ctx: FromStatementContext): LogicalPlan = withOrigin(ctx) {
    val from = visitFromClause(ctx.fromClause)
    val selects = ctx.fromStatementBody.asScala.map { body =>
      withFromStatementBody(body, from).
        // Add organization statements.
        optionalMap(body.queryOrganization)(withQueryResultClauses)
    }
    // If there are multiple SELECT just UNION them together into one query.
    if (selects.length == 1) {
      selects.head
    } else {
      Union(selects.toSeq)
    }
  }

  /**
   * Create a named logical plan.
   *
   * This is only used for Common Table Expressions.
   */
  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    val subQuery: LogicalPlan = plan(ctx.query).optionalMap(ctx.columnAliases)(
      (columnAliases, plan) =>
        UnresolvedSubqueryColumnAliases(visitIdentifierList(columnAliases), plan)
    )
    SubqueryAlias(ctx.name.getText, subQuery)
  }

  /**
   * Create a logical plan which allows for multiple inserts using one 'from' statement. These
   * queries have the following SQL form:
   * {{{
   *   [WITH cte...]?
   *   FROM src
   *   [INSERT INTO tbl1 SELECT *]+
   * }}}
   * For example:
   * {{{
   *   FROM db.tbl1 A
   *   INSERT INTO dbo.tbl1 SELECT * WHERE A.value = 10 LIMIT 5
   *   INSERT INTO dbo.tbl2 SELECT * WHERE A.value = 12
   * }}}
   * This (Hive) feature cannot be combined with set-operators.
   */
  override def visitMultiInsertQuery(ctx: MultiInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    val from = visitFromClause(ctx.fromClause)

    // Build the insert clauses.
    val inserts = ctx.multiInsertQueryBody.asScala.map { body =>
      withInsertInto(body.insertInto,
        withFromStatementBody(body.fromStatementBody, from).
          optionalMap(body.fromStatementBody.queryOrganization)(withQueryResultClauses))
    }

    // If there are multiple INSERTS just UNION them together into one query.
    if (inserts.length == 1) {
      inserts.head
    } else {
      Union(inserts.toSeq)
    }
  }

  /**
   * Create a logical plan for a regular (single-insert) query.
   */
  override def visitSingleInsertQuery(
                                       ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    withInsertInto(
      ctx.insertInto(),
      plan(ctx.queryTerm).optionalMap(ctx.queryOrganization)(withQueryResultClauses))
  }

  /**
   * Parameters used for writing query to a table:
   * (UnresolvedRelation, tableColumnList, partitionKeys, ifPartitionNotExists).
   */
  type InsertTableParams = (UnresolvedRelation, Seq[String], Map[String, Option[String]], Boolean)

  /**
   * Parameters used for writing query to a directory: (isLocal, CatalogStorageFormat, provider).
   */
  type InsertDirParams = (Boolean, CatalogStorageFormat, Option[String])

  /**
   * Add an
   * {{{
   *   INSERT OVERWRITE TABLE tableIdentifier [partitionSpec [IF NOT EXISTS]]? [identifierList]
   *   INSERT INTO [TABLE] tableIdentifier [partitionSpec]  [identifierList]
   *   INSERT OVERWRITE [LOCAL] DIRECTORY STRING [rowFormat] [createFileFormat]
   *   INSERT OVERWRITE [LOCAL] DIRECTORY [STRING] tableProvider [OPTIONS tablePropertyList]
   * }}}
   * operation to logical plan
   */
  private def withInsertInto(
                              ctx: InsertIntoContext,
                              query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    ctx match {
      case table: InsertIntoTableContext =>
        val (relation, cols, partition, ifPartitionNotExists) = visitInsertIntoTable(table)
        InsertIntoStatement(
          relation,
          partition,
          cols,
          query,
          overwrite = false,
          ifPartitionNotExists)
      case table: InsertOverwriteTableContext =>
        val (relation, cols, partition, ifPartitionNotExists) = visitInsertOverwriteTable(table)
        InsertIntoStatement(
          relation,
          partition,
          cols,
          query,
          overwrite = true,
          ifPartitionNotExists)
      case dir: InsertOverwriteDirContext =>
        val (isLocal, storage, provider) = visitInsertOverwriteDir(dir)
        InsertIntoDir(isLocal, storage, provider, query, overwrite = true)
      case hiveDir: InsertOverwriteHiveDirContext =>
        val (isLocal, storage, provider) = visitInsertOverwriteHiveDir(hiveDir)
        InsertIntoDir(isLocal, storage, provider, query, overwrite = true)
      case _ =>
        throw new ParseException("Invalid InsertIntoContext", ctx)
    }
  }

  /**
   * Add an INSERT INTO TABLE operation to the logical plan.
   */
  override def visitInsertIntoTable(
                                     ctx: InsertIntoTableContext): InsertTableParams = withOrigin(ctx) {
    val cols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Nil)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    if (ctx.EXISTS != null) {
      operationNotAllowed("INSERT INTO ... IF NOT EXISTS", ctx)
    }

    (createUnresolvedRelation(ctx.multipartIdentifier), cols, partitionKeys, false)
  }

  /**
   * Add an INSERT OVERWRITE TABLE operation to the logical plan.
   */
  override def visitInsertOverwriteTable(
                                          ctx: InsertOverwriteTableContext): InsertTableParams = withOrigin(ctx) {
    assert(ctx.OVERWRITE() != null)
    val cols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Nil)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    val dynamicPartitionKeys: Map[String, Option[String]] = partitionKeys.filter(_._2.isEmpty)
    if (ctx.EXISTS != null && dynamicPartitionKeys.nonEmpty) {
      operationNotAllowed("IF NOT EXISTS with dynamic partitions: " +
        dynamicPartitionKeys.keys.mkString(", "), ctx)
    }

    (createUnresolvedRelation(ctx.multipartIdentifier), cols, partitionKeys, ctx.EXISTS() != null)
  }

  /**
   * Write to a directory, returning a [[InsertIntoDir]] logical plan.
   */
  override def visitInsertOverwriteDir(
                                        ctx: InsertOverwriteDirContext): InsertDirParams = withOrigin(ctx) {
    throw new ParseException("INSERT OVERWRITE DIRECTORY is not supported", ctx)
  }

  /**
   * Write to a directory, returning a [[InsertIntoDir]] logical plan.
   */
  override def visitInsertOverwriteHiveDir(
                                            ctx: InsertOverwriteHiveDirContext): InsertDirParams = withOrigin(ctx) {
    throw new ParseException("INSERT OVERWRITE DIRECTORY is not supported", ctx)
  }

  private def getTableAliasWithoutColumnAlias(
                                               ctx: TableAliasContext, op: String): Option[String] = {
    if (ctx == null) {
      None
    } else {
      val ident = ctx.strictIdentifier()
      if (ctx.identifierList() != null) {
        throw new ParseException(s"Columns aliases are not allowed in $op.", ctx.identifierList())
      }
      if (ident != null) Some(ident.getText) else None
    }
  }

  override def visitDeleteFromTable(
                                     ctx: DeleteFromTableContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedRelation(ctx.multipartIdentifier())
    val tableAlias = getTableAliasWithoutColumnAlias(ctx.tableAlias(), "DELETE")
    val aliasedTable = tableAlias.map(SubqueryAlias(_, table)).getOrElse(table)
    val predicate = if (ctx.whereClause() != null) {
      Some(expression(ctx.whereClause().booleanExpression()))
    } else {
      None
    }
    DeleteFromTable(aliasedTable, predicate.get)
  }

  override def visitUpdateTable(ctx: UpdateTableContext): LogicalPlan = withOrigin(ctx) {
    val table = createUnresolvedRelation(ctx.multipartIdentifier())
    val tableAlias = getTableAliasWithoutColumnAlias(ctx.tableAlias(), "UPDATE")
    val aliasedTable = tableAlias.map(SubqueryAlias(_, table)).getOrElse(table)
    val assignments = withAssignments(ctx.setClause().assignmentList())
    val predicate = if (ctx.whereClause() != null) {
      Some(expression(ctx.whereClause().booleanExpression()))
    } else {
      None
    }

    UpdateTable(aliasedTable, assignments, predicate)
  }

  private def withAssignments(assignCtx: AssignmentListContext): Seq[Assignment] =
    withOrigin(assignCtx) {
      assignCtx.assignment().asScala.map { assign =>
        Assignment(UnresolvedAttribute(visitMultipartIdentifier(assign.key)),
          expression(assign.value))
      }.toSeq
    }

  override def visitMergeIntoTable(ctx: MergeIntoTableContext): LogicalPlan = withOrigin(ctx) {
    val targetTable = createUnresolvedRelation(ctx.target)
    val targetTableAlias = getTableAliasWithoutColumnAlias(ctx.targetAlias, "MERGE")
    val aliasedTarget = targetTableAlias.map(SubqueryAlias(_, targetTable)).getOrElse(targetTable)

    val sourceTableOrQuery = if (ctx.source != null) {
      createUnresolvedRelation(ctx.source)
    } else if (ctx.sourceQuery != null) {
      visitQuery(ctx.sourceQuery)
    } else {
      throw new ParseException("Empty source for merge: you should specify a source" +
        " table/subquery in merge.", ctx.source)
    }
    val sourceTableAlias = getTableAliasWithoutColumnAlias(ctx.sourceAlias, "MERGE")
    val aliasedSource =
      sourceTableAlias.map(SubqueryAlias(_, sourceTableOrQuery)).getOrElse(sourceTableOrQuery)

    val mergeCondition = expression(ctx.mergeCondition)

    val matchedActions = ctx.matchedClause().asScala.map {
      clause => {
        if (clause.matchedAction().DELETE() != null) {
          DeleteAction(Option(clause.matchedCond).map(expression))
        } else if (clause.matchedAction().UPDATE() != null) {
          val condition = Option(clause.matchedCond).map(expression)
          if (clause.matchedAction().ASTERISK() != null) {
            UpdateStarAction(condition)
          } else {
            UpdateAction(condition, withAssignments(clause.matchedAction().assignmentList()))
          }
        } else {
          // It should not be here.
          throw new ParseException(s"Unrecognized matched action: ${clause.matchedAction().getText}",
            clause.matchedAction())
        }
      }
    }
    val notMatchedActions = ctx.notMatchedClause().asScala.map {
      clause => {
        if (clause.notMatchedAction().INSERT() != null) {
          val condition = Option(clause.notMatchedCond).map(expression)
          if (clause.notMatchedAction().ASTERISK() != null) {
            InsertStarAction(condition)
          } else {
            val columns = clause.notMatchedAction().columns.multipartIdentifier()
              .asScala.map(attr => UnresolvedAttribute(visitMultipartIdentifier(attr)))
            val values = clause.notMatchedAction().expression().asScala.map(expression)
            if (columns.size != values.size) {
              throw new ParseException("The number of inserted values cannot match the fields.",
                clause.notMatchedAction())
            }
            InsertAction(condition, columns.zip(values).map(kv => Assignment(kv._1, kv._2)).toSeq)
          }
        } else {
          // It should not be here.
          throw new ParseException(s"Unrecognized not matched action: ${clause.notMatchedAction().getText}",
            clause.notMatchedAction())
        }
      }
    }
    if (matchedActions.isEmpty && notMatchedActions.isEmpty) {
      throw new ParseException("There must be at least one WHEN clause in a MERGE statement", ctx)
    }
    // children being empty means that the condition is not set
    val matchedActionSize = matchedActions.length
    if (matchedActionSize >= 2 && !matchedActions.init.forall(_.condition.nonEmpty)) {
      throw new ParseException("When there are more than one MATCHED clauses in a MERGE " +
        "statement, only the last MATCHED clause can omit the condition.", ctx)
    }
    val notMatchedActionSize = notMatchedActions.length
    if (notMatchedActionSize >= 2 && !notMatchedActions.init.forall(_.condition.nonEmpty)) {
      throw new ParseException("When there are more than one NOT MATCHED clauses in a MERGE " +
        "statement, only the last NOT MATCHED clause can omit the condition.", ctx)
    }

    MergeIntoTable(
      aliasedTarget,
      aliasedSource,
      mergeCondition,
      matchedActions.toSeq,
      notMatchedActions.toSeq,
      Seq.empty,
      false
    )
  }

  /**
   * Create a partition specification map.
   */
  override def visitPartitionSpec(
                                   ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val legacyNullAsString =
      conf.getConf(SQLConf.LEGACY_PARSE_NULL_PARTITION_SPEC_AS_STRING_LITERAL)
    val parts = ctx.partitionVal.asScala.map { pVal =>
      val name = pVal.identifier.getText
      val value = Option(pVal.constant).map(v => visitStringConstant(v, legacyNullAsString))
      name -> value
    }
    // Before calling `toMap`, we check duplicated keys to avoid silently ignore partition values
    // in partition spec like PARTITION(a='1', b='2', a='3'). The real semantical check for
    // partition columns will be done in analyzer.
    if (conf.caseSensitiveAnalysis) {
      checkDuplicateKeys(parts.toSeq, ctx)
    } else {
      checkDuplicateKeys(parts.map(kv => kv._1.toLowerCase(Locale.ROOT) -> kv._2).toSeq, ctx)
    }
    parts.toMap
  }

  /**
   * Create a partition specification map without optional values.
   */
  protected def visitNonOptionalPartitionSpec(
                                               ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).map {
      case (key, None) => throw new ParseException(s"Found an empty partition key '$key'.", ctx)
      case (key, Some(value)) => key -> value
    }
  }

  /**
   * Convert a constant of any type into a string. This is typically used in DDL commands, and its
   * main purpose is to prevent slight differences due to back to back conversions i.e.:
   * String -> Literal -> String.
   */
  protected def visitStringConstant(
                                     ctx: ConstantContext,
                                     legacyNullAsString: Boolean): String = withOrigin(ctx) {
    expression(ctx) match {
      case Literal(null, _) if !legacyNullAsString => null
      case l@Literal(null, _) => l.toString
      case l: Literal =>
        // TODO For v2 commands, we will cast the string back to its actual value,
        //  which is a waste and can be improved in the future.
        Cast(l, StringType, Some(conf.sessionLocalTimeZone)).eval().toString
      case other =>
        throw new IllegalArgumentException(s"Only literals are allowed in the " +
          s"partition spec, but got ${other.sql}")
    }
  }

  /**
   * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan. These
   * clauses determine the shape (ordering/partitioning/rows) of the query result.
   */
  private def withQueryResultClauses(
                                      ctx: QueryOrganizationContext,
                                      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
    val withOrder = if (
      !order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // ORDER BY ...
      Sort(order.asScala.map(visitSortItem).toSeq, global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ...
      Sort(sort.asScala.map(visitSortItem).toSeq, global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // DISTRIBUTE BY ...
      withRepartitionByExpression(ctx, expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ... DISTRIBUTE BY ...
      Sort(
        sort.asScala.map(visitSortItem).toSeq,
        global = false,
        withRepartitionByExpression(ctx, expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      // CLUSTER BY ...
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        withRepartitionByExpression(ctx, expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // [EMPTY]
      query
    } else {
      throw new ParseException(
        "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // WINDOWS
    val withWindow = withOrder.optionalMap(windowClause)(withWindowClause)

    // LIMIT
    // - LIMIT ALL is the same as omitting the LIMIT clause
    withWindow.optional(limit) {
      Limit(typedVisit(limit), withWindow)
    }
  }

  /**
   * Create a clause for DISTRIBUTE BY.
   */
  protected def withRepartitionByExpression(
                                             ctx: QueryOrganizationContext,
                                             expressions: Seq[Expression],
                                             query: LogicalPlan): LogicalPlan = {
    RepartitionByExpression(expressions, query, None)
  }

  override def visitTransformQuerySpecification(
                                                 ctx: TransformQuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation().optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withTransformQuerySpecification(
      ctx,
      ctx.transformClause,
      ctx.lateralView,
      ctx.whereClause,
      ctx.aggregationClause,
      ctx.havingClause,
      ctx.windowClause,
      from
    )
  }

  override def visitRegularQuerySpecification(
                                               ctx: RegularQuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation().optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    withSelectQuerySpecification(
      ctx,
      ctx.selectClause,
      ctx.lateralView,
      ctx.whereClause,
      ctx.aggregationClause,
      ctx.havingClause,
      ctx.windowClause,
      from
    )
  }

  override def visitNamedExpressionSeq(
                                        ctx: NamedExpressionSeqContext): Seq[Expression] = {
    Option(ctx).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
  }

  override def visitExpressionSeq(ctx: ExpressionSeqContext): Seq[Expression] = {
    Option(ctx).toSeq
      .flatMap(_.expression.asScala)
      .map(typedVisit[Expression])
  }

  /**
   * Create a logical plan using a having clause.
   */
  private def withHavingClause(
                                ctx: HavingClauseContext, plan: LogicalPlan): LogicalPlan = {
    // Note that we add a cast to non-predicate expressions. If the expression itself is
    // already boolean, the optimizer will get rid of the unnecessary cast.
    val predicate = expression(ctx.booleanExpression) match {
      case p: Predicate => p
      case e => Cast(e, BooleanType)
    }
    UnresolvedHaving(predicate, plan)
  }

  /**
   * Create a logical plan using a where clause.
   */
  private def withWhereClause(ctx: WhereClauseContext, plan: LogicalPlan): LogicalPlan = {
    Filter(expression(ctx.booleanExpression), plan)
  }

  /**
   * Add a hive-style transform (SELECT TRANSFORM/MAP/REDUCE) query specification to a logical plan.
   */
  private def withTransformQuerySpecification(
                                               ctx: ParserRuleContext,
                                               transformClause: TransformClauseContext,
                                               lateralView: java.util.List[LateralViewContext],
                                               whereClause: WhereClauseContext,
                                               aggregationClause: AggregationClauseContext,
                                               havingClause: HavingClauseContext,
                                               windowClause: WindowClauseContext,
                                               relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    if (transformClause.setQuantifier != null) {
      throw new ParseException("TRANSFORM does not support DISTINCT/ALL in inputs", transformClause.setQuantifier)
    }
    // Create the attributes.
    val (attributes, schemaLess) = if (transformClause.colTypeList != null) {
      // Typed return columns.
      (DataTypeUtils.toAttributes(createSchema(transformClause.colTypeList)), false)
    } else if (transformClause.identifierSeq != null) {
      // Untyped return columns.
      val attrs = visitIdentifierSeq(transformClause.identifierSeq).map { name =>
        AttributeReference(name, StringType, nullable = true)()
      }
      (attrs, false)
    } else {
      (Seq(AttributeReference("key", StringType)(),
        AttributeReference("value", StringType)()), true)
    }

    val plan = visitCommonSelectQueryClausePlan(
      relation,
      visitExpressionSeq(transformClause.expressionSeq),
      lateralView,
      whereClause,
      aggregationClause,
      havingClause,
      windowClause,
      isDistinct = false)

    ScriptTransformation(
      string(transformClause.script),
      attributes,
      plan,
      withScriptIOSchema(
        ctx,
        transformClause.inRowFormat,
        transformClause.recordWriter,
        transformClause.outRowFormat,
        transformClause.recordReader,
        schemaLess
      )
    )
  }

  /**
   * Add a regular (SELECT) query specification to a logical plan. The query specification
   * is the core of the logical plan, this is where sourcing (FROM clause), projection (SELECT),
   * aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
   *
   * Note that query hints are ignored (both by the parser and the builder).
   */
  private def withSelectQuerySpecification(
                                            ctx: ParserRuleContext,
                                            selectClause: SelectClauseContext,
                                            lateralView: java.util.List[LateralViewContext],
                                            whereClause: WhereClauseContext,
                                            aggregationClause: AggregationClauseContext,
                                            havingClause: HavingClauseContext,
                                            windowClause: WindowClauseContext,
                                            relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val isDistinct = selectClause.setQuantifier() != null &&
      selectClause.setQuantifier().DISTINCT() != null

    val plan = visitCommonSelectQueryClausePlan(
      relation,
      visitNamedExpressionSeq(selectClause.namedExpressionSeq),
      lateralView,
      whereClause,
      aggregationClause,
      havingClause,
      windowClause,
      isDistinct)

    // Hint
    selectClause.hints.asScala.foldRight(plan)(withHints)
  }

  def visitCommonSelectQueryClausePlan(
                                        relation: LogicalPlan,
                                        expressions: Seq[Expression],
                                        lateralView: java.util.List[LateralViewContext],
                                        whereClause: WhereClauseContext,
                                        aggregationClause: AggregationClauseContext,
                                        havingClause: HavingClauseContext,
                                        windowClause: WindowClauseContext,
                                        isDistinct: Boolean): LogicalPlan = {
    // Add lateral views.
    val withLateralView = lateralView.asScala.foldLeft(relation)(withGenerate)

    // Add where.
    val withFilter = withLateralView.optionalMap(whereClause)(withWhereClause)

    // Add aggregation or a project.
    val namedExpressions = expressions.map {
      case e: NamedExpression => e
      case e: Expression => UnresolvedAlias(e)
    }

    def createProject() = if (namedExpressions.nonEmpty) {
      Project(namedExpressions, withFilter)
    } else {
      withFilter
    }

    val withProject = if (aggregationClause == null && havingClause != null) {
      if (conf.getConf(SQLConf.LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE)) {
        // If the legacy conf is set, treat HAVING without GROUP BY as WHERE.
        val predicate = expression(havingClause.booleanExpression) match {
          case p: Predicate => p
          case e => Cast(e, BooleanType)
        }
        Filter(predicate, createProject())
      } else {
        // According to SQL standard, HAVING without GROUP BY means global aggregate.
        withHavingClause(havingClause, Aggregate(Nil, namedExpressions, withFilter))
      }
    } else if (aggregationClause != null) {
      val aggregate = withAggregationClause(aggregationClause, namedExpressions, withFilter)
      aggregate.optionalMap(havingClause)(withHavingClause)
    } else {
      // When hitting this branch, `having` must be null.
      createProject()
    }

    // Distinct
    val withDistinct = if (isDistinct) {
      Distinct(withProject)
    } else {
      withProject
    }

    // Window
    val withWindow = withDistinct.optionalMap(windowClause)(withWindowClause)

    withWindow
  }

  // Script Transform's input/output format.
  type ScriptIOFormat =
    (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])

  protected def getRowFormatDelimited(ctx: RowFormatDelimitedContext): ScriptIOFormat = {
    // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
    // expects a seq of pairs in which the old parsers' token names are used as keys.
    // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
    // retrieving the key value pairs ourselves.
    val entries = entry("TOK_TABLEROWFORMATFIELD", ctx.fieldsTerminatedBy) ++
      entry("TOK_TABLEROWFORMATCOLLITEMS", ctx.collectionItemsTerminatedBy) ++
      entry("TOK_TABLEROWFORMATMAPKEYS", ctx.keysTerminatedBy) ++
      entry("TOK_TABLEROWFORMATNULL", ctx.nullDefinedAs) ++
      Option(ctx.linesSeparatedBy).toSeq.map { token =>
        val value = string(token)
        validate(
          value == "\n",
          s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
          ctx)
        "TOK_TABLEROWFORMATLINES" -> value
      }

    (entries, None, Seq.empty, None)
  }

  /**
   * Create a [[ScriptInputOutputSchema]].
   */
  protected def withScriptIOSchema(
                                    ctx: ParserRuleContext,
                                    inRowFormat: RowFormatContext,
                                    recordWriter: Token,
                                    outRowFormat: RowFormatContext,
                                    recordReader: Token,
                                    schemaLess: Boolean): ScriptInputOutputSchema = {

    def format(fmt: RowFormatContext): ScriptIOFormat = fmt match {
      case c: RowFormatDelimitedContext =>
        getRowFormatDelimited(c)

      case c: RowFormatSerdeContext =>
        throw new ParseException("TRANSFORM with serde is only supported in hive mode", ctx)

      // SPARK-32106: When there is no definition about format, we return empty result
      // to use a built-in default Serde in SparkScriptTransformationExec.
      case null =>
        (Nil, None, Seq.empty, None)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) = format(inRowFormat)

    val (outFormat, outSerdeClass, outSerdeProps, writer) = format(outRowFormat)

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  /**
   * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
   * separated) relations here, these get converted into a single plan by condition-less inner join.
   */
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      val right = plan(relation.relationPrimary)
      val join = right.optionalMap(left) { (left, right) =>
        if (relation.LATERAL != null) {
          if (!relation.relationPrimary.isInstanceOf[AliasedQueryContext]) {
            throw new ParseException(s"LATERAL can only be used with subquery", relation.relationPrimary)
          }
          LateralJoin(left, LateralSubquery(right), Inner, None)
        } else {
          Join(left, right, Inner, None, JoinHint.NONE)
        }
      }
      withJoinRelations(join, relation)
    }
    if (ctx.pivotClause() != null) {
      if (!ctx.lateralView.isEmpty) {
        throw new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx)
      }
      withPivot(ctx.pivotClause, from)
    } else {
      ctx.lateralView.asScala.foldLeft(from)(withGenerate)
    }
  }

  /**
   * Connect two queries by a Set operator.
   *
   * Supported Set operators are:
   * - UNION [ DISTINCT | ALL ]
   * - EXCEPT [ DISTINCT | ALL ]
   * - MINUS [ DISTINCT | ALL ]
   * - INTERSECT [DISTINCT | ALL]
   */
  override def visitSetOperation(ctx: SetOperationContext): LogicalPlan = withOrigin(ctx) {
    val left = plan(ctx.left)
    val right = plan(ctx.right)
    val all = Option(ctx.setQuantifier()).exists(_.ALL != null)
    ctx.operator.getType match {
      case HoodieSqlBaseParser.UNION if all =>
        Union(left, right)
      case HoodieSqlBaseParser.UNION =>
        Distinct(Union(left, right))
      case HoodieSqlBaseParser.INTERSECT if all =>
        Intersect(left, right, isAll = true)
      case HoodieSqlBaseParser.INTERSECT =>
        Intersect(left, right, isAll = false)
      case HoodieSqlBaseParser.EXCEPT if all =>
        Except(left, right, isAll = true)
      case HoodieSqlBaseParser.EXCEPT =>
        Except(left, right, isAll = false)
      case HoodieSqlBaseParser.SETMINUS if all =>
        Except(left, right, isAll = true)
      case HoodieSqlBaseParser.SETMINUS =>
        Except(left, right, isAll = false)
    }
  }

  /**
   * Add a [[WithWindowDefinition]] operator to a logical plan.
   */
  private def withWindowClause(
                                ctx: WindowClauseContext,
                                query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    val baseWindowTuples = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.name.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }
    baseWindowTuples.groupBy(_._1).foreach { kv =>
      if (kv._2.size > 1) {
        throw new ParseException(s"The definition of window '${kv._1}' is repetitive", ctx)
      }
    }
    val baseWindowMap = baseWindowTuples.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    val windowMapView = baseWindowMap.mapValues {
      case WindowSpecReference(name) =>
        baseWindowMap.get(name) match {
          case Some(spec: WindowSpecDefinition) =>
            spec
          case Some(ref) =>
            throw new ParseException(s"Window reference '$name' is not a window specification", ctx)
          case None =>
            throw new ParseException(s"Cannot resolve window reference '$name'", ctx)
        }
      case spec: WindowSpecDefinition => spec
    }

    // Note that mapValues creates a view instead of materialized map. We force materialization by
    // mapping over identity.
    WithWindowDefinition(windowMapView.map(identity).toMap, query, forPipeSQL = false)
  }

  /**
   * Add an [[Aggregate]] to a logical plan.
   */
  private def withAggregationClause(
                                     ctx: AggregationClauseContext,
                                     selectExpressions: Seq[NamedExpression],
                                     query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    if (ctx.groupingExpressionsWithGroupingAnalytics.isEmpty) {
      val groupByExpressions = expressionList(ctx.groupingExpressions)
      if (ctx.GROUPING != null) {
        // GROUP BY ... GROUPING SETS (...)
        // `groupByExpressions` can be non-empty for Hive compatibility. It may add extra grouping
        // expressions that do not exist in GROUPING SETS (...), and the value is always null.
        // For example, `SELECT a, b, c FROM ... GROUP BY a, b, c GROUPING SETS (a, b)`, the output
        // of column `c` is always null.
        val groupingSets =
        ctx.groupingSet.asScala.map(_.expression.asScala.map(e => expression(e)).toSeq)
        Aggregate(Seq(GroupingSets(groupingSets.toSeq, groupByExpressions)),
          selectExpressions, query)
      } else {
        // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
        val mappedGroupByExpressions = if (ctx.CUBE != null) {
          Seq(Cube(groupByExpressions.map(Seq(_))))
        } else if (ctx.ROLLUP != null) {
          Seq(Rollup(groupByExpressions.map(Seq(_))))
        } else {
          groupByExpressions
        }
        Aggregate(mappedGroupByExpressions, selectExpressions, query)
      }
    } else {
      val groupByExpressions =
        ctx.groupingExpressionsWithGroupingAnalytics.asScala
          .map(groupByExpr => {
            val groupingAnalytics = groupByExpr.groupingAnalytics
            if (groupingAnalytics != null) {
              visitGroupingAnalytics(groupingAnalytics)
            } else {
              expression(groupByExpr.expression)
            }
          })
      Aggregate(groupByExpressions.toSeq, selectExpressions, query)
    }
  }

  override def visitGroupingAnalytics(
                                       groupingAnalytics: GroupingAnalyticsContext): BaseGroupingSets = {
    val groupingSets = groupingAnalytics.groupingSet.asScala
      .map(_.expression.asScala.map(e => expression(e)).toSeq)
    if (groupingAnalytics.CUBE != null) {
      // CUBE(A, B, (A, B), ()) is not supported.
      if (groupingSets.exists(_.isEmpty)) {
        throw new ParseException(s"Empty set in CUBE grouping sets is not supported.", groupingAnalytics)
      }
      Cube(groupingSets.toSeq)
    } else if (groupingAnalytics.ROLLUP != null) {
      // ROLLUP(A, B, (A, B), ()) is not supported.
      if (groupingSets.exists(_.isEmpty)) {
        throw new ParseException(s"Empty set in ROLLUP grouping sets is not supported.", groupingAnalytics)
      }
      Rollup(groupingSets.toSeq)
    } else {
      assert(groupingAnalytics.GROUPING != null && groupingAnalytics.SETS != null)
      val groupingSets = groupingAnalytics.groupingElement.asScala.flatMap { expr =>
        val groupingAnalytics = expr.groupingAnalytics()
        if (groupingAnalytics != null) {
          visitGroupingAnalytics(groupingAnalytics).selectedGroupByExprs
        } else {
          Seq(expr.groupingSet().expression().asScala.map(e => expression(e)).toSeq)
        }
      }
      GroupingSets(groupingSets.toSeq)
    }
  }

  /**
   * Add [[UnresolvedHint]]s to a logical plan.
   */
  private def withHints(
                         ctx: HintContext,
                         query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    var plan = query
    ctx.hintStatements.asScala.reverse.foreach { stmt =>
      plan = UnresolvedHint(stmt.hintName.getText,
        stmt.parameters.asScala.map(expression).toSeq, plan)
    }
    plan
  }

  /**
   * Add a [[Pivot]] to a logical plan.
   */
  private def withPivot(
                         ctx: PivotClauseContext,
                         query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    val aggregates = Option(ctx.aggregates).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])
    val pivotColumn = if (ctx.pivotColumn.identifiers.size == 1) {
      UnresolvedAttribute.quoted(ctx.pivotColumn.identifier.getText)
    } else {
      CreateStruct(
        ctx.pivotColumn.identifiers.asScala.map(
          identifier => UnresolvedAttribute.quoted(identifier.getText)).toSeq)
    }
    val pivotValues = ctx.pivotValues.asScala.map(visitPivotValue)
    Pivot(None, pivotColumn, pivotValues.toSeq, aggregates, query)
  }

  /**
   * Create a Pivot column value with or without an alias.
   */
  override def visitPivotValue(ctx: PivotValueContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      Alias(e, ctx.identifier.getText)()
    } else {
      e
    }
  }

  /**
   * Add a [[Generate]] (Lateral View) to a logical plan.
   */
  private def withGenerate(
                            query: LogicalPlan,
                            ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
    val expressions = expressionList(ctx.expression)
    Generate(
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      unrequiredChildIndex = Nil,
      outer = ctx.OUTER != null,
      // scalastyle:off caselocale
      Some(ctx.tblName.getText.toLowerCase),
      // scalastyle:on caselocale
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.quoted).toSeq,
      query)
  }

  /**
   * Create a single relation referenced in a FROM clause. This method is used when a part of the
   * join condition is nested, for example:
   * {{{
   *   select * from t1 join (t2 cross join t3) on col1 = col2
   * }}}
   */
  override def visitRelation(ctx: RelationContext): LogicalPlan = withOrigin(ctx) {
    withJoinRelations(plan(ctx.relationPrimary), ctx)
  }

  /**
   * Join one more [[LogicalPlan]]s to the current logical plan.
   */
  private def withJoinRelations(base: LogicalPlan, ctx: RelationContext): LogicalPlan = {
    ctx.joinRelation.asScala.foldLeft(base) { (left, join) =>
      withOrigin(join) {
        val baseJoinType = join.joinType match {
          case null => Inner
          case jt if jt.CROSS != null => Cross
          case jt if jt.FULL != null => FullOuter
          case jt if jt.SEMI != null => LeftSemi
          case jt if jt.ANTI != null => LeftAnti
          case jt if jt.LEFT != null => LeftOuter
          case jt if jt.RIGHT != null => RightOuter
          case _ => Inner
        }

        if (join.LATERAL != null && !join.right.isInstanceOf[AliasedQueryContext]) {
          throw new ParseException(s"LATERAL can only be used with subquery", join.right)
        }

        // Resolve the join type and join condition
        val (joinType, condition) = Option(join.joinCriteria) match {
          case Some(c) if c.USING != null =>
            if (join.LATERAL != null) {
              throw new ParseException("LATERAL join with USING join is not supported", ctx)
            }
            (UsingJoin(baseJoinType, visitIdentifierList(c.identifierList)), None)
          case Some(c) if c.booleanExpression != null =>
            (baseJoinType, Option(expression(c.booleanExpression)))
          case Some(c) =>
            throw new ParseException(s"Unimplemented joinCriteria: $c", ctx)
          case None if join.NATURAL != null =>
            if (join.LATERAL != null) {
              throw new ParseException("LATERAL join with NATURAL join is not supported", ctx)
            }
            if (baseJoinType == Cross) {
              throw new ParseException("NATURAL CROSS JOIN is not supported", ctx)
            }
            (NaturalJoin(baseJoinType), None)
          case None =>
            (baseJoinType, None)
        }
        if (join.LATERAL != null) {
          if (!Seq(Inner, Cross, LeftOuter).contains(joinType)) {
            throw new ParseException(s"Unsupported LATERAL join type ${joinType.toString}", ctx)
          }
          LateralJoin(left, LateralSubquery(plan(join.right)), joinType, condition)
        } else {
          Join(left, plan(join.right), joinType, condition, JoinHint.NONE)
        }
      }
    }
  }

  /**
   * Add a [[Sample]] to a logical plan.
   *
   * This currently supports the following sampling methods:
   * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
   * - TABLESAMPLE(x PERCENT): Sample the table down to the given percentage. Note that percentages
   * are defined as a number between 0 and 100.
   * - TABLESAMPLE(BUCKET x OUT OF y): Sample the table down to a 'x' divided by 'y' fraction.
   */
  private def withSample(ctx: SampleContext, query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
      // function takes X PERCENT as the input and the range of X is [0, 100], we need to
      // adjust the fraction.
      val eps = RandomSampler.roundingEpsilon
      validate(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
        s"Sampling fraction ($fraction) must be on interval [0, 1]",
        ctx)
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, query)
    }

    if (ctx.sampleMethod() == null) {
      throw new ParseException("TABLESAMPLE does not accept empty inputs.", ctx)
    }

    ctx.sampleMethod() match {
      case ctx: SampleByRowsContext =>
        Limit(expression(ctx.expression), query)

      case ctx: SampleByPercentileContext =>
        val fraction = ctx.percentage.getText.toDouble
        val sign = if (ctx.negativeSign == null) 1 else -1
        sample(sign * fraction / 100.0d)

      case ctx: SampleByBytesContext =>
        val bytesStr = ctx.bytes.getText
        if (bytesStr.matches("[0-9]+[bBkKmMgG]")) {
          throw new ParseException(s"TABLESAMPLE(byteLengthLiteral) is not supported", ctx)
        } else {
          throw new ParseException(s"$bytesStr is not a valid byte length literal, " +
            "expected syntax: DIGIT+ ('B' | 'K' | 'M' | 'G')", ctx)
        }

      case ctx: SampleByBucketContext if ctx.ON() != null =>
        if (ctx.identifier != null) {
          throw new ParseException(s"TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported", ctx)
        } else {
          throw new ParseException(s"TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported", ctx)
        }

      case ctx: SampleByBucketContext =>
        sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble)
    }
  }

  /**
   * Create a logical plan for a sub-query.
   */
  override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.query)
  }

  /**
   * Create an un-aliased table reference. This is typically used for top-level table references,
   * for example:
   * {{{
   *   INSERT INTO db.tbl2
   *   TABLE db.tbl1
   * }}}
   */
  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
    UnresolvedRelation(visitMultipartIdentifier(ctx.multipartIdentifier))
  }

  /**
   * Create a table-valued function call with arguments, e.g. range(1000)
   */
  override def visitTableValuedFunction(ctx: TableValuedFunctionContext)
  : LogicalPlan = withOrigin(ctx) {
    val func = ctx.functionTable
    val aliases = if (func.tableAlias.identifierList != null) {
      visitIdentifierList(func.tableAlias.identifierList)
    } else {
      Seq.empty
    }
    val name = getFunctionIdentifier(func.functionName)
    if (name.database.nonEmpty) {
      operationNotAllowed(s"table valued function cannot specify database name: $name", ctx)
    }

    val tvf = UnresolvedTableValuedFunction(name, func.expression.asScala.map(expression).toSeq)

    val tvfAliases = if (aliases.nonEmpty) UnresolvedTVFAliases(name, tvf, aliases) else tvf

    tvfAliases.optionalMap(func.tableAlias.strictIdentifier)(aliasPlan)
  }

  /**
   * Create an inline table (a virtual table in Hive parlance).
   */
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val rows = ctx.expression.asScala.map { e =>
      expression(e) match {
        // inline table comes in two styles:
        // style 1: values (1), (2), (3)  -- multiple columns are supported
        // style 2: values 1, 2, 3  -- only a single column is supported here
        case struct: CreateNamedStruct => struct.valExprs // style 1
        case child => Seq(child) // style 2
      }
    }

    val aliases = if (ctx.tableAlias.identifierList != null) {
      visitIdentifierList(ctx.tableAlias.identifierList)
    } else {
      Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
    }

    val table = UnresolvedInlineTable(aliases, rows.toSeq)
    table.optionalMap(ctx.tableAlias.strictIdentifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
   * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks. We could add alias names for output columns, for example:
   * {{{
   *   SELECT a, b, c, d FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)
   * }}}
   */
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.relation).optionalMap(ctx.sample)(withSample)
    mayApplyAliasPlan(ctx.tableAlias, relation)
  }

  /**
   * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
   * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks. We could add alias names for output columns, for example:
   * {{{
   *   SELECT col1, col2 FROM testData AS t(col1, col2)
   * }}}
   */
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    val relation = plan(ctx.query).optionalMap(ctx.sample)(withSample)
    if (ctx.tableAlias.strictIdentifier == null) {
      // For un-aliased subqueries, use a default alias name that is not likely to conflict with
      // normal subquery names, so that parent operators can only access the columns in subquery by
      // unqualified names. Users can still use this special qualifier to access columns if they
      // know it, but that's not recommended.
      SubqueryAlias("__auto_generated_subquery_name", relation)
    } else {
      mayApplyAliasPlan(ctx.tableAlias, relation)
    }
  }

  /**
   * Create an alias ([[SubqueryAlias]]) for a [[LogicalPlan]].
   */
  private def aliasPlan(alias: ParserRuleContext, plan: LogicalPlan): LogicalPlan = {
    SubqueryAlias(alias.getText, plan)
  }

  /**
   * If aliases specified in a FROM clause, create a subquery alias ([[SubqueryAlias]]) and
   * column aliases for a [[LogicalPlan]].
   */
  private def mayApplyAliasPlan(tableAlias: TableAliasContext, plan: LogicalPlan): LogicalPlan = {
    if (tableAlias.strictIdentifier != null) {
      val alias = tableAlias.strictIdentifier.getText
      if (tableAlias.identifierList != null) {
        val columnNames = visitIdentifierList(tableAlias.identifierList)
        SubqueryAlias(alias, UnresolvedSubqueryColumnAliases(columnNames, plan))
      } else {
        SubqueryAlias(alias, plan)
      }
    } else {
      plan
    }
  }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
   * Create a Sequence of Strings for an identifier list.
   */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.ident.asScala.map(_.getText).toSeq
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */

  /**
   * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
  override def visitTableIdentifier(
                                     ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a [[FunctionIdentifier]] from a 'functionName' or 'databaseName'.'functionName' pattern.
   */
  override def visitFunctionIdentifier(
                                        ctx: FunctionIdentifierContext): FunctionIdentifier = withOrigin(ctx) {
    FunctionIdentifier(ctx.function.getText, Option(ctx.db).map(_.getText))
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */

  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /**
   * Create sequence of expressions from the given sequence of contexts.
   */
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression).toSeq
  }

  /**
   * Create a star (i.e. all) expression; this selects all elements (in the specified object).
   * Both un-targeted (global) and targeted aliases are supported.
   */
  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    UnresolvedStar(Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText).toSeq))
  }

  /**
   * Create an aliased expression if an alias is specified. Both single and multi-aliases are
   * supported.
   */
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.name != null) {
      Alias(e, ctx.name.getText)()
    } else if (ctx.identifierList != null) {
      MultiAlias(e, visitIdentifierList(ctx.identifierList))
    } else {
      e
    }
  }

  /**
   * Combine a number of boolean expressions into a balanced expression tree. These expressions are
   * either combined by a logical [[And]] or a logical [[Or]].
   *
   * A balanced binary tree is created because regular left recursive trees cause considerable
   * performance degradations and can cause stack overflows.
   */
  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case HoodieSqlBaseParser.AND => And.apply _
      case HoodieSqlBaseParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left

    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }

    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseMap(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }

    reduceToExpressionTree(0, expressions.size - 1)
  }

  /**
   * Invert a boolean expression.
   */
  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    Not(expression(ctx.booleanExpression()))
  }

  /**
   * Create a filtering correlated sub-query (EXISTS).
   */
  override def visitExists(ctx: ExistsContext): Expression = {
    Exists(plan(ctx.query))
  }

  /**
   * Create a comparison expression. This compares two expressions. The following comparison
   * operators are supported:
   * - Equal: '=' or '=='
   * - Null-safe Equal: '<=>'
   * - Not Equal: '<>' or '!='
   * - Less than: '<'
   * - Less then or Equal: '<='
   * - Greater than: '>'
   * - Greater then or Equal: '>='
   */
  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case HoodieSqlBaseParser.EQ =>
        EqualTo(left, right)
      case HoodieSqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case HoodieSqlBaseParser.NEQ | HoodieSqlBaseParser.NEQJ =>
        Not(EqualTo(left, right))
      case HoodieSqlBaseParser.LT =>
        LessThan(left, right)
      case HoodieSqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case HoodieSqlBaseParser.GT =>
        GreaterThan(left, right)
      case HoodieSqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  /**
   * Create a predicated expression. A predicated expression is a normal expression with a
   * predicate attached to it, for example:
   * {{{
   *    a + 1 IS NULL
   * }}}
   */
  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  /**
   * Add a predicate to the given expression. Supported expressions are:
   * - (NOT) BETWEEN
   * - (NOT) IN
   * - (NOT) LIKE (ANY | SOME | ALL)
   * - (NOT) RLIKE
   * - IS (NOT) NULL.
   * - IS (NOT) (TRUE | FALSE | UNKNOWN)
   * - IS (NOT) DISTINCT FROM
   */
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    def getValueExpressions(e: Expression): Seq[Expression] = e match {
      case c: CreateNamedStruct => c.valExprs
      case other => Seq(other)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case HoodieSqlBaseParser.BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case HoodieSqlBaseParser.IN if ctx.query != null =>
        invertIfNotDefined(InSubquery(getValueExpressions(e), ListQuery(plan(ctx.query))))
      case HoodieSqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression).toSeq))
      case HoodieSqlBaseParser.LIKE =>
        Option(ctx.quantifier).map(_.getType) match {
          case Some(HoodieSqlBaseParser.ANY) | Some(HoodieSqlBaseParser.SOME) =>
            validate(!ctx.expression.isEmpty, "Expected something between '(' and ')'.", ctx)
            val expressions = expressionList(ctx.expression)
            if (expressions.forall(_.foldable) && expressions.forall(_.dataType == StringType)) {
              // If there are many pattern expressions, will throw StackOverflowError.
              // So we use LikeAny or NotLikeAny instead.
              val patterns = expressions.map(_.eval(EmptyRow).asInstanceOf[UTF8String])
              ctx.NOT match {
                case null => LikeAny(e, patterns)
                case _ => NotLikeAny(e, patterns)
              }
            } else {
              ctx.expression.asScala.map(expression)
                .map(p => invertIfNotDefined(new Like(e, p))).toSeq.reduceLeft(Or)
            }
          case Some(HoodieSqlBaseParser.ALL) =>
            validate(!ctx.expression.isEmpty, "Expected something between '(' and ')'.", ctx)
            val expressions = expressionList(ctx.expression)
            if (expressions.forall(_.foldable) && expressions.forall(_.dataType == StringType)) {
              // If there are many pattern expressions, will throw StackOverflowError.
              // So we use LikeAll or NotLikeAll instead.
              val patterns = expressions.map(_.eval(EmptyRow).asInstanceOf[UTF8String])
              ctx.NOT match {
                case null => LikeAll(e, patterns)
                case _ => NotLikeAll(e, patterns)
              }
            } else {
              ctx.expression.asScala.map(expression)
                .map(p => invertIfNotDefined(new Like(e, p))).toSeq.reduceLeft(And)
            }
          case _ =>
            val escapeChar = Option(ctx.escapeChar).map(string).map { str =>
              if (str.length != 1) {
                throw new ParseException("Invalid escape string. Escape string must contain only one character.", ctx)
              }
              str.charAt(0)
            }.getOrElse('\\')
            invertIfNotDefined(Like(e, expression(ctx.pattern), escapeChar))
        }
      case HoodieSqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case HoodieSqlBaseParser.NULL if ctx.NOT != null =>
        IsNotNull(e)
      case HoodieSqlBaseParser.NULL =>
        IsNull(e)
      case HoodieSqlBaseParser.TRUE => ctx.NOT match {
        case null => EqualNullSafe(e, Literal(true))
        case _ => Not(EqualNullSafe(e, Literal(true)))
      }
      case HoodieSqlBaseParser.FALSE => ctx.NOT match {
        case null => EqualNullSafe(e, Literal(false))
        case _ => Not(EqualNullSafe(e, Literal(false)))
      }
      case HoodieSqlBaseParser.UNKNOWN => ctx.NOT match {
        case null => IsUnknown(e)
        case _ => IsNotUnknown(e)
      }
      case HoodieSqlBaseParser.DISTINCT if ctx.NOT != null =>
        EqualNullSafe(e, expression(ctx.right))
      case HoodieSqlBaseParser.DISTINCT =>
        Not(EqualNullSafe(e, expression(ctx.right)))
    }
  }

  /**
   * Create a binary arithmetic expression. The following arithmetic operators are supported:
   * - Multiplication: '*'
   * - Division: '/'
   * - Hive Long Division: 'DIV'
   * - Modulo: '%'
   * - Addition: '+'
   * - Subtraction: '-'
   * - Binary AND: '&'
   * - Binary XOR
   * - Binary OR: '|'
   */
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case HoodieSqlBaseParser.ASTERISK =>
        Multiply(left, right)
      case HoodieSqlBaseParser.SLASH =>
        Divide(left, right)
      case HoodieSqlBaseParser.PERCENT =>
        Remainder(left, right)
      case HoodieSqlBaseParser.DIV =>
        IntegralDivide(left, right)
      case HoodieSqlBaseParser.PLUS =>
        Add(left, right)
      case HoodieSqlBaseParser.MINUS =>
        Subtract(left, right)
      case HoodieSqlBaseParser.CONCAT_PIPE =>
        Concat(left :: right :: Nil)
      case HoodieSqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case HoodieSqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case HoodieSqlBaseParser.PIPE =>
        BitwiseOr(left, right)
    }
  }

  /**
   * Create a unary arithmetic expression. The following arithmetic operators are supported:
   * - Plus: '+'
   * - Minus: '-'
   * - Bitwise Not: '~'
   */
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case HoodieSqlBaseParser.PLUS =>
        UnaryPositive(value)
      case HoodieSqlBaseParser.MINUS =>
        UnaryMinus(value)
      case HoodieSqlBaseParser.TILDE =>
        BitwiseNot(value)
    }
  }

  override def visitCurrentLike(ctx: CurrentLikeContext): Expression = withOrigin(ctx) {
    if (conf.ansiEnabled) {
      ctx.name.getType match {
        case HoodieSqlBaseParser.CURRENT_DATE =>
          CurrentDate()
        case HoodieSqlBaseParser.CURRENT_TIMESTAMP =>
          CurrentTimestamp()
        case HoodieSqlBaseParser.CURRENT_USER =>
          CurrentUser()
      }
    } else {
      // If the parser is not in ansi mode, we should return `UnresolvedAttribute`, in case there
      // are columns named `CURRENT_DATE` or `CURRENT_TIMESTAMP`.
      UnresolvedAttribute.quoted(ctx.name.getText)
    }
  }

  /**
   * Create a [[Cast]] expression.
   */
  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    val rawDataType = typedVisit[DataType](ctx.dataType())
    val dataType = CharVarcharUtils.replaceCharVarcharWithStringForCast(rawDataType)
    val cast = ctx.name.getType match {
      case HoodieSqlBaseParser.CAST =>
        Cast(expression(ctx.expression), dataType)

      case HoodieSqlBaseParser.TRY_CAST =>
        Cast(expression(ctx.expression), dataType, evalMode = EvalMode.TRY)
    }
    cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
    cast
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
  override def visitStruct(ctx: StructContext): Expression = withOrigin(ctx) {
    CreateStruct.create(ctx.argument.asScala.map(expression).toSeq)
  }

  /**
   * Create a [[First]] expression.
   */
  override def visitFirst(ctx: FirstContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    First(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create a [[Last]] expression.
   */
  override def visitLast(ctx: LastContext): Expression = withOrigin(ctx) {
    val ignoreNullsExpr = ctx.IGNORE != null
    Last(expression(ctx.expression), ignoreNullsExpr).toAggregateExpression()
  }

  /**
   * Create a Position expression.
   */
  override def visitPosition(ctx: PositionContext): Expression = withOrigin(ctx) {
    new StringLocate(expression(ctx.substr), expression(ctx.str))
  }

  /**
   * Create a Extract expression.
   */
  override def visitExtract(ctx: ExtractContext): Expression = withOrigin(ctx) {
    val arguments = Seq(Literal(ctx.field.getText), expression(ctx.source))
    UnresolvedFunction("extract", arguments, isDistinct = false)
  }

  /**
   * Create a Substring/Substr expression.
   */
  override def visitSubstring(ctx: SubstringContext): Expression = withOrigin(ctx) {
    if (ctx.len != null) {
      Substring(expression(ctx.str), expression(ctx.pos), expression(ctx.len))
    } else {
      new Substring(expression(ctx.str), expression(ctx.pos))
    }
  }

  /**
   * Create a Trim expression.
   */
  override def visitTrim(ctx: TrimContext): Expression = withOrigin(ctx) {
    val srcStr = expression(ctx.srcStr)
    val trimStr = Option(ctx.trimStr).map(expression)
    Option(ctx.trimOption).map(_.getType).getOrElse(HoodieSqlBaseParser.BOTH) match {
      case HoodieSqlBaseParser.BOTH =>
        StringTrim(srcStr, trimStr)
      case HoodieSqlBaseParser.LEADING =>
        StringTrimLeft(srcStr, trimStr)
      case HoodieSqlBaseParser.TRAILING =>
        StringTrimRight(srcStr, trimStr)
      case other =>
        throw new ParseException("Function trim doesn't support with " +
          s"type $other. Please use BOTH, LEADING or TRAILING as trim type", ctx)
    }
  }

  /**
   * Create a Overlay expression.
   */
  override def visitOverlay(ctx: OverlayContext): Expression = withOrigin(ctx) {
    val input = expression(ctx.input)
    val replace = expression(ctx.replace)
    val position = expression(ctx.position)
    val lengthOpt = Option(ctx.length).map(expression)
    lengthOpt match {
      case Some(length) => Overlay(input, replace, position, length)
      case None => new Overlay(input, replace, position)
    }
  }

  /**
   * Create a (windowed) Function expression.
   */
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    // Create the function call.
    val name = ctx.functionName.getText
    val isDistinct = Option(ctx.setQuantifier()).exists(_.DISTINCT != null)
    // Call `toSeq`, otherwise `ctx.argument.asScala.map(expression)` is `Buffer` in Scala 2.13
    val arguments = ctx.argument.asScala.map(expression).toSeq match {
      case Seq(UnresolvedStar(None))
        if name.toLowerCase(Locale.ROOT) == "count" && !isDistinct =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val filter = Option(ctx.where).map(expression(_))
    val ignoreNulls =
      Option(ctx.nullsOption).map(_.getType == HoodieSqlBaseParser.IGNORE).getOrElse(false)
    val function = UnresolvedFunction(
      getFunctionMultiparts(ctx.functionName), arguments, isDistinct, filter, ignoreNulls)

    // Check if the function is evaluated in a windowed context.
    ctx.windowSpec match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  /**
   * Create a function database (optional) and name pair.
   */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    visitFunctionName(ctx, ctx.identifier().asScala.map(_.getText).toSeq)
  }

  /**
   * Create a function database (optional) and name pair.
   */
  private def visitFunctionName(ctx: ParserRuleContext, texts: Seq[String]): FunctionIdentifier = {
    texts match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))
      case Seq(fn) => FunctionIdentifier(fn, None)
      case other =>
        throw new ParseException(s"Unsupported function name '${texts.mkString(".")}'", ctx)
    }
  }

  /**
   * Get a function identifier consist by database (optional) and name.
   */
  protected def getFunctionIdentifier(ctx: FunctionNameContext): FunctionIdentifier = {
    if (ctx.qualifiedName != null) {
      visitFunctionName(ctx.qualifiedName)
    } else {
      FunctionIdentifier(ctx.getText, None)
    }
  }

  protected def getFunctionMultiparts(ctx: FunctionNameContext): Seq[String] = {
    if (ctx.qualifiedName != null) {
      ctx.qualifiedName().identifier().asScala.map(_.getText).toSeq
    } else {
      Seq(ctx.getText)
    }
  }

  /**
   * Create an [[LambdaFunction]].
   */
  override def visitLambda(ctx: LambdaContext): Expression = withOrigin(ctx) {
    val arguments = ctx.identifier().asScala.map { name =>
      UnresolvedNamedLambdaVariable(UnresolvedAttribute.quoted(name.getText).nameParts)
    }
    val function = expression(ctx.expression).transformUp {
      case a: UnresolvedAttribute => UnresolvedNamedLambdaVariable(a.nameParts)
    }
    LambdaFunction(function, arguments.toSeq)
  }

  /**
   * Create a reference to a window frame, i.e. [[WindowSpecReference]].
   */
  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.name.getText)
  }

  /**
   * Create a window definition, i.e. [[WindowSpecDefinition]].
   */
  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    // CLUSTER BY ... | PARTITION BY ... ORDER BY ...
    val partition = ctx.partition.asScala.map(expression)
    val order = ctx.sortItem.asScala.map(visitSortItem)

    // RANGE/ROWS BETWEEN ...
    val frameSpecOption = Option(ctx.windowFrame).map { frame =>
      val frameType = frame.frameType.getType match {
        case HoodieSqlBaseParser.RANGE => RangeFrame
        case HoodieSqlBaseParser.ROWS => RowFrame
      }

      SpecifiedWindowFrame(
        frameType,
        visitFrameBound(frame.start),
        Option(frame.end).map(visitFrameBound).getOrElse(CurrentRow))
    }

    WindowSpecDefinition(
      partition.toSeq,
      order.toSeq,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  /**
   * Create or resolve a frame boundary expressions.
   */
  override def visitFrameBound(ctx: FrameBoundContext): Expression = withOrigin(ctx) {
    def value: Expression = {
      val e = expression(ctx.expression)
      validate(e.resolved && e.foldable, "Frame bound value must be a literal.", ctx)
      e
    }

    ctx.boundType.getType match {
      case HoodieSqlBaseParser.PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case HoodieSqlBaseParser.PRECEDING =>
        UnaryMinus(value)
      case HoodieSqlBaseParser.CURRENT =>
        CurrentRow
      case HoodieSqlBaseParser.FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case HoodieSqlBaseParser.FOLLOWING =>
        value
    }
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.namedExpression().asScala.map(expression).toSeq)
  }

  /**
   * Create a [[ScalarSubquery]] expression.
   */
  override def visitSubqueryExpression(
                                        ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
    ScalarSubquery(plan(ctx.query))
  }

  /**
   * Create a value based [[CaseWhen]] expression. This has the following SQL form:
   * {{{
   *   CASE [expression]
   *    WHEN [value] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   */
  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.value)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches.toSeq, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
   * {{{
   *   CASE
   *    WHEN [predicate] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   *
   * @param ctx the parse tree
   * */
  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches.toSeq, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Currently only regex in expressions of SELECT statements are supported; in other
   * places, e.g., where `(a)?+.+` = 2, regex are not meaningful.
   */
  private def canApplyRegex(ctx: ParserRuleContext): Boolean = withOrigin(ctx) {
    var parent = ctx.getParent
    var rtn = false
    while (parent != null) {
      if (parent.isInstanceOf[NamedExpressionContext]) {
        rtn = true
      }
      parent = parent.getParent
    }
    rtn
  }

  /**
   * Create a dereference expression. The return type depends on the type of the parent.
   * If the parent is an [[UnresolvedAttribute]], it can be a [[UnresolvedAttribute]] or
   * a [[UnresolvedRegex]] for regex quoted in ``; if the parent is some other expression,
   * it can be [[UnresolvedExtractValue]].
   */
  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case unresolved_attr@UnresolvedAttribute(nameParts) =>
        ctx.fieldName.getStart.getText match {
          case escapedIdentifier(columnNameRegex)
            if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
            UnresolvedRegex(columnNameRegex, Some(unresolved_attr.name),
              conf.caseSensitiveAnalysis)
          case _ =>
            UnresolvedAttribute(nameParts :+ attr)
        }
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  /**
   * Create an [[UnresolvedAttribute]] expression or a [[UnresolvedRegex]] if it is a regex
   * quoted in ``
   */
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    ctx.getStart.getText match {
      case escapedIdentifier(columnNameRegex)
        if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
        UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)
      case _ =>
        UnresolvedAttribute.quoted(ctx.getText)
    }

  }

  /**
   * Create an [[UnresolvedExtractValue]] expression, this is used for subscript access to an array.
   */
  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  /**
   * Create an expression for an expression between parentheses. This is need because the ANTLR
   * visitor cannot automatically convert the nested context into an expression.
   */
  override def visitParenthesizedExpression(
                                             ctx: ParenthesizedExpressionContext): Expression = withOrigin(ctx) {
    expression(ctx.expression)
  }

  /**
   * Create a [[SortOrder]] expression.
   */
  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    val direction = if (ctx.DESC != null) {
      Descending
    } else {
      Ascending
    }
    val nullOrdering = if (ctx.FIRST != null) {
      NullsFirst
    } else if (ctx.LAST != null) {
      NullsLast
    } else {
      direction.defaultNullOrdering
    }
    SortOrder(expression(ctx.expression), direction, nullOrdering, Seq.empty)
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp, Interval and Binary typed literals are supported.
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase(Locale.ROOT)

    def toLiteral[T](f: UTF8String => Option[T], t: DataType): Literal = {
      f(UTF8String.fromString(value)).map(Literal(_, t)).getOrElse {
        throw new ParseException(s"Cannot parse the $valueType value: $value", ctx)
      }
    }

    def constructTimestampLTZLiteral(value: String): Literal = {
      val zoneId = getZoneId(conf.sessionLocalTimeZone)
      val specialTs = convertSpecialTimestamp(value, zoneId).map(Literal(_, TimestampType))
      specialTs.getOrElse(toLiteral(stringToTimestamp(_, zoneId), TimestampType))
    }

    try {
      valueType match {
        case "DATE" =>
          val zoneId = getZoneId(conf.sessionLocalTimeZone)
          val specialDate = convertSpecialDate(value, zoneId).map(Literal(_, DateType))
          specialDate.getOrElse(toLiteral(stringToDate, DateType))
        // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
        case "TIMESTAMP_NTZ" if isTesting =>
          convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
            .map(Literal(_, TimestampNTZType))
            .getOrElse(toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType))
        case "TIMESTAMP_LTZ" if isTesting =>
          constructTimestampLTZLiteral(value)
        case "TIMESTAMP" =>
          SQLConf.get.timestampType match {
            case TimestampNTZType =>
              convertSpecialTimestampNTZ(value, getZoneId(conf.sessionLocalTimeZone))
                .map(Literal(_, TimestampNTZType))
                .getOrElse {
                  val containsTimeZonePart =
                    DateTimeUtils.parseTimestampString(UTF8String.fromString(value))._2.isDefined
                  // If the input string contains time zone part, return a timestamp with local time
                  // zone literal.
                  if (containsTimeZonePart) {
                    constructTimestampLTZLiteral(value)
                  } else {
                    toLiteral(stringToTimestampWithoutTimeZone, TimestampNTZType)
                  }
                }

            case TimestampType =>
              constructTimestampLTZLiteral(value)
          }

        case "INTERVAL" =>
          val interval = try {
            IntervalUtils.stringToInterval(UTF8String.fromString(value))
          } catch {
            case e: IllegalArgumentException =>
              val ex = new ParseException(s"Cannot parse the INTERVAL value: $value", ctx)
              ex.setStackTrace(e.getStackTrace)
              throw ex
          }
          if (!conf.legacyIntervalEnabled) {
            val units = value
              .split("\\s")
              .map(_.toLowerCase(Locale.ROOT).stripSuffix("s"))
              .filter(s => s != "interval" && s.matches("[a-z]+"))
            constructMultiUnitsIntervalLiteral(ctx, interval, units)
          } else {
            Literal(interval, CalendarIntervalType)
          }
        case "X" =>
          val padding = if (value.length % 2 != 0) "0" else ""
          Literal(DatatypeConverter.parseHexBinary(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a decimal literal for a regular decimal number or a scientific decimal number.
   */
  override def visitLegacyDecimalLiteral(
                                          ctx: LegacyDecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a double literal for number with an exponent, e.g. 1E-30
   */
  override def visitExponentLiteral(ctx: ExponentLiteralContext): Literal = {
    numericLiteral(ctx, ctx.getText, /* exponent values don't have a suffix */
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
                              ctx: NumberContext,
                              rawStrippedQualifier: String,
                              minValue: BigDecimal,
                              maxValue: BigDecimal,
                              typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal $rawStrippedQualifier does not " +
          s"fit in range [$minValue, $maxValue] for type $typeName", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Float Literal expression.
   */
  override def visitFloatLiteral(ctx: FloatLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Float.MinValue, Float.MaxValue, FloatType.simpleString)(_.toFloat)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(x => stringWithoutUnescape(x.getSymbol)).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  /**
   * Create an [[UnresolvedRelation]] from a multi-part identifier context.
   */
  private def createUnresolvedRelation(
                                        ctx: MultipartIdentifierContext): UnresolvedRelation = withOrigin(ctx) {
    UnresolvedRelation(visitMultipartIdentifier(ctx))
  }

  /**
   * Construct an [[Literal]] from [[CalendarInterval]] and
   * units represented as a [[Seq]] of [[String]].
   */
  private def constructMultiUnitsIntervalLiteral(
                                                  ctx: ParserRuleContext,
                                                  calendarInterval: CalendarInterval,
                                                  units: Seq[String]): Literal = {
    var yearMonthFields = Set.empty[Byte]
    var dayTimeFields = Set.empty[Byte]
    for (unit <- units) {
      if (YearMonthIntervalType.stringToField.contains(unit)) {
        yearMonthFields += YearMonthIntervalType.stringToField(unit)
      } else if (DayTimeIntervalType.stringToField.contains(unit)) {
        dayTimeFields += DayTimeIntervalType.stringToField(unit)
      } else if (unit == "week") {
        dayTimeFields += DayTimeIntervalType.DAY
      } else {
        assert(unit == "millisecond" || unit == "microsecond")
        dayTimeFields += DayTimeIntervalType.SECOND
      }
    }
    if (yearMonthFields.nonEmpty) {
      if (dayTimeFields.nonEmpty) {
        val literalStr = source(ctx)
        throw new ParseException(s"Cannot mix year-month and day-time fields: $literalStr", ctx)
      }
      Literal(
        calendarInterval.months,
        YearMonthIntervalType(yearMonthFields.min, yearMonthFields.max)
      )
    } else {
      Literal(
        IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS),
        DayTimeIntervalType(dayTimeFields.min, dayTimeFields.max))
    }
  }

  /**
   * Create a [[CalendarInterval]] or ANSI interval literal expression.
   * Two syntaxes are supported:
   * - multiple unit value pairs, for instance: interval 2 months 2 days.
   * - from-to unit, for instance: interval '1-2' year to month.
   */
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val calendarInterval = parseIntervalLiteral(ctx)
    if (ctx.errorCapturingUnitToUnitInterval != null && !conf.legacyIntervalEnabled) {
      // Check the `to` unit to distinguish year-month and day-time intervals because
      // `CalendarInterval` doesn't have enough info. For instance, new CalendarInterval(0, 0, 0)
      // can be derived from INTERVAL '0-0' YEAR TO MONTH as well as from
      // INTERVAL '0 00:00:00' DAY TO SECOND.
      val fromUnit =
      ctx.errorCapturingUnitToUnitInterval.body.from.getText.toLowerCase(Locale.ROOT)
      val toUnit = ctx.errorCapturingUnitToUnitInterval.body.to.getText.toLowerCase(Locale.ROOT)
      if (toUnit == "month") {
        assert(calendarInterval.days == 0 && calendarInterval.microseconds == 0)
        val start = YearMonthIntervalType.stringToField(fromUnit)
        Literal(calendarInterval.months, YearMonthIntervalType(start, YearMonthIntervalType.MONTH))
      } else {
        assert(calendarInterval.months == 0)
        val micros = IntervalUtils.getDuration(calendarInterval, TimeUnit.MICROSECONDS)
        val start = DayTimeIntervalType.stringToField(fromUnit)
        val end = DayTimeIntervalType.stringToField(toUnit)
        Literal(micros, DayTimeIntervalType(start, end))
      }
    } else if (ctx.errorCapturingMultiUnitsInterval != null && !conf.legacyIntervalEnabled) {
      val units =
        ctx.errorCapturingMultiUnitsInterval.body.unit.asScala.map(
          _.getText.toLowerCase(Locale.ROOT).stripSuffix("s")).toSeq
      constructMultiUnitsIntervalLiteral(ctx, calendarInterval, units)
    } else {
      Literal(calendarInterval, CalendarIntervalType)
    }
  }

  /**
   * Create a [[CalendarInterval]] object
   */
  protected def parseIntervalLiteral(ctx: IntervalContext): CalendarInterval = withOrigin(ctx) {
    if (ctx.errorCapturingMultiUnitsInterval != null) {
      val innerCtx = ctx.errorCapturingMultiUnitsInterval
      if (innerCtx.unitToUnitInterval != null) {
        throw new ParseException("Can only have a single from-to unit in the interval literal syntax", innerCtx.unitToUnitInterval)
      }
      visitMultiUnitsInterval(innerCtx.multiUnitsInterval)
    } else if (ctx.errorCapturingUnitToUnitInterval != null) {
      val innerCtx = ctx.errorCapturingUnitToUnitInterval
      if (innerCtx.error1 != null || innerCtx.error2 != null) {
        val errorCtx = if (innerCtx.error1 != null) innerCtx.error1 else innerCtx.error2
        throw new ParseException("Can only have a single from-to unit in the interval literal syntax", errorCtx)
      }
      visitUnitToUnitInterval(innerCtx.body)
    } else {
      throw new ParseException("at least one time unit should be given for interval literal", ctx)
    }
  }

  /**
   * Creates a [[CalendarInterval]] with multiple unit value pairs, e.g. 1 YEAR 2 DAYS.
   */
  override def visitMultiUnitsInterval(ctx: MultiUnitsIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val units = ctx.unit.asScala
      val values = ctx.intervalValue().asScala
      try {
        assert(units.length == values.length)
        val kvs = units.indices.map { i =>
          val u = units(i).getText
          val v = if (values(i).STRING() != null) {
            val value = string(values(i).STRING())
            // SPARK-32840: For invalid cases, e.g. INTERVAL '1 day 2' hour,
            // INTERVAL 'interval 1' day, we need to check ahead before they are concatenated with
            // units and become valid ones, e.g. '1 day 2 hour'.
            // Ideally, we only ensure the value parts don't contain any units here.
            if (value.exists(Character.isLetter)) {
              throw new ParseException("Can only use numbers in the interval value part for" +
                s" multiple unit value pairs interval form, but got invalid value: $value", ctx)
            }
            if (values(i).MINUS() == null) {
              value
            } else {
              value.startsWith("-") match {
                case true => value.replaceFirst("-", "")
                case false => s"-$value"
              }
            }
          } else {
            values(i).getText
          }
          UTF8String.fromString(" " + v + " " + u)
        }
        IntervalUtils.stringToInterval(UTF8String.concat(kvs: _*))
      } catch {
        case i: IllegalArgumentException =>
          val e = new ParseException(i.getMessage, ctx)
          e.setStackTrace(i.getStackTrace)
          throw e
      }
    }
  }

  /**
   * Creates a [[CalendarInterval]] with from-to unit, e.g. '2-1' YEAR TO MONTH.
   */
  override def visitUnitToUnitInterval(ctx: UnitToUnitIntervalContext): CalendarInterval = {
    withOrigin(ctx) {
      val value = Option(ctx.intervalValue.STRING).map(string).map { interval =>
        if (ctx.intervalValue().MINUS() == null) {
          interval
        } else {
          interval.startsWith("-") match {
            case true => interval.replaceFirst("-", "")
            case false => s"-$interval"
          }
        }
      }.getOrElse {
        throw new ParseException("The value of from-to unit must be a string", ctx.intervalValue)
      }
      try {
        val from = ctx.from.getText.toLowerCase(Locale.ROOT)
        val to = ctx.to.getText.toLowerCase(Locale.ROOT)
        (from, to) match {
          case ("year", "month") =>
            IntervalUtils.fromYearMonthString(value)
          case ("day", "hour") | ("day", "minute") | ("day", "second") | ("hour", "minute") |
               ("hour", "second") | ("minute", "second") =>
            IntervalUtils.fromDayTimeString(value,
              DayTimeIntervalType.stringToField(from), DayTimeIntervalType.stringToField(to))
          case _ =>
            throw new ParseException(s"Intervals FROM $from TO $to are not supported.", ctx)
        }
      } catch {
        // Handle Exceptions thrown by CalendarInterval
        case e: IllegalArgumentException =>
          val pe = new ParseException(e.getMessage, ctx)
          pe.setStackTrace(e.getStackTrace)
          throw pe
      }
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val dataType = ctx.identifier.getText.toLowerCase(Locale.ROOT)
    (dataType, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float" | "real", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => SQLConf.get.timestampType
      // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
      case ("timestamp_ntz", Nil) if isTesting => TimestampNTZType
      case ("timestamp_ltz", Nil) if isTesting => TimestampType
      case ("string", Nil) => StringType
      case ("character" | "char", length :: Nil) => CharType(length.getText.toInt)
      case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
      case ("binary", Nil) => BinaryType
      case ("decimal" | "dec" | "numeric", Nil) => DecimalType.USER_DEFAULT
      case ("decimal" | "dec" | "numeric", precision :: Nil) =>
        DecimalType(precision.getText.toInt, 0)
      case ("decimal" | "dec" | "numeric", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case ("void", Nil) => NullType
      case ("interval", Nil) => CalendarIntervalType
      case (dt, params) =>
        val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
        throw new ParseException(s"DataType $dtStr is not supported.", ctx)
    }
  }

  override def visitYearMonthIntervalDataType(ctx: YearMonthIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = YearMonthIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = YearMonthIntervalType.stringToField(endStr)
      if (end <= start) {
        throw new ParseException(s"Intervals FROM $startStr TO $endStr are not supported.", ctx)
      }
      YearMonthIntervalType(start, end)
    } else {
      YearMonthIntervalType(start)
    }
  }

  override def visitDayTimeIntervalDataType(ctx: DayTimeIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = DayTimeIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = DayTimeIntervalType.stringToField(endStr)
      if (end <= start) {
        throw new ParseException(s"Intervals FROM $startStr TO $endStr are not supported.", ctx)
      }
      DayTimeIntervalType(start, end)
    } else {
      DayTimeIntervalType(start)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case HoodieSqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case HoodieSqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case HoodieSqlBaseParser.STRUCT =>
        StructType(Option(ctx.complexColTypeList).toSeq.flatMap(visitComplexColTypeList))
    }
  }

  /**
   * Create top level table schema.
   */
  protected def createSchema(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType).toSeq
  }

  /**
   * Create a top level [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    Option(commentSpec()).map(visitCommentSpec).foreach {
      builder.putString("comment", _)
    }

    StructField(
      name = colName.getText,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = NULL == null,
      metadata = builder.build())
  }

  /**
   * Create a [[StructType]] from a sequence of [[StructField]]s.
   */
  protected def createStructType(ctx: ComplexColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitComplexColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(
                                        ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType).toSeq
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(
      name = identifier.getText,
      dataType = typedVisit(dataType()),
      nullable = NULL == null)
    Option(commentSpec).map(visitCommentSpec).map(structField.withComment).getOrElse(structField)
  }

  /**
   * Create a location string.
   */
  override def visitLocationSpec(ctx: LocationSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
   * Create an optional location string.
   */
  protected def visitLocationSpecList(ctx: java.util.List[LocationSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitLocationSpec)
  }

  /**
   * Create a comment string.
   */
  override def visitCommentSpec(ctx: CommentSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  /**
   * Create an optional comment string.
   */
  protected def visitCommentSpecList(ctx: java.util.List[CommentSpecContext]): Option[String] = {
    ctx.asScala.headOption.map(visitCommentSpec)
  }

  /**
   * Create a [[BucketSpec]].
   */
  override def visitBucketSpec(ctx: BucketSpecContext): BucketSpec = withOrigin(ctx) {
    BucketSpec(
      ctx.INTEGER_VALUE.getText.toInt,
      visitIdentifierList(ctx.identifierList),
      Option(ctx.orderedIdentifierList)
        .toSeq
        .flatMap(_.orderedIdentifier.asScala)
        .map { orderedIdCtx =>
          Option(orderedIdCtx.ordering).map(_.getText).foreach { dir =>
            if (dir.toLowerCase(Locale.ROOT) != "asc") {
              operationNotAllowed(s"Column ordering must be ASC, was '$dir'", ctx)
            }
          }

          orderedIdCtx.ident.getText
        })
  }

  /**
   * Convert a table property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitTablePropertyList(
                                       ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = visitTablePropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties.toSeq, ctx)
    properties.toMap
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props
  }

  /**
   * Parse a list of keys from a [[TablePropertyListContext]], assuming no values are specified.
   */
  def visitPropertyKeys(ctx: TablePropertyListContext): Seq[String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v != null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values should not be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx)
    }
    props.keys.toSeq
  }

  /**
   * A table property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a table property
   * identifier.
   */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * A table property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
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

  /**
   * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
   */
  type TableHeader = (Seq[String], Boolean, Boolean, Boolean)

  /**
   * Type to keep track of table clauses:
   * - partition transforms
   * - partition columns
   * - bucketSpec
   * - properties
   * - options
   * - location
   * - comment
   * - serde
   *
   * Note: Partition transforms are based on existing table schema definition. It can be simple
   * column names, or functions like `year(date_col)`. Partition columns are column names with data
   * types like `i INT`, which should be appended to the existing table schema.
   */
  type TableClauses = (
    Seq[Transform], Seq[StructField], Option[BucketSpec], Map[String, String],
      Map[String, String], Option[String], Option[String], Option[SerdeInfo])

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
                                       ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText).toSeq
    (multipartIdentifier, temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Validate a replace table statement and return the [[TableIdentifier]].
   */
  override def visitReplaceTableHeader(
                                        ctx: ReplaceTableHeaderContext): TableHeader = withOrigin(ctx) {
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText).toSeq
    (multipartIdentifier, false, false, false)
  }

  /**
   * Parse a qualified name to a multipart name.
   */
  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText).toSeq
  }

  /**
   * Parse a list of transforms or columns.
   */
  override def visitPartitionFieldList(
                                        ctx: PartitionFieldListContext): (Seq[Transform], Seq[StructField]) = withOrigin(ctx) {
    val (transforms, columns) = ctx.fields.asScala.map {
      case transform: PartitionTransformContext =>
        (Some(visitPartitionTransform(transform)), None)
      case field: PartitionColumnContext =>
        (None, Some(visitColType(field.colType)))
    }.unzip

    (transforms.flatten.toSeq, columns.flatten.toSeq)
  }

  override def visitPartitionTransform(
                                        ctx: PartitionTransformContext): Transform = withOrigin(ctx) {
    def getFieldReference(
                           ctx: ApplyTransformContext,
                           arg: V2Expression): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      arg match {
        case ref: FieldReference =>
          ref
        case nonRef =>
          throw new ParseException(s"Expected a column reference for transform $name: $nonRef.describe", ctx)
      }
    }

    def getSingleFieldReference(
                                 ctx: ApplyTransformContext,
                                 arguments: Seq[V2Expression]): FieldReference = {
      lazy val name: String = ctx.identifier.getText
      if (arguments.size > 1) {
        throw new ParseException(s"Too many arguments for transform $name", ctx)
      } else if (arguments.isEmpty) {
        throw

          new ParseException(s"Not enough arguments for transform $name", ctx)
      } else {
        getFieldReference(ctx, arguments.head)
      }
    }

    ctx.transform match {
      case identityCtx: IdentityTransformContext =>
        IdentityTransform(FieldReference(typedVisit[Seq[String]](identityCtx.qualifiedName)))

      case applyCtx: ApplyTransformContext =>
        val arguments = applyCtx.argument.asScala.map(visitTransformArgument).toSeq

        applyCtx.identifier.getText match {
          case "bucket" =>
            val numBuckets: Int = arguments.head match {
              case LiteralValue(shortValue, ShortType) =>
                shortValue.asInstanceOf[Short].toInt
              case LiteralValue(intValue, IntegerType) =>
                intValue.asInstanceOf[Int]
              case LiteralValue(longValue, LongType) =>
                longValue.asInstanceOf[Long].toInt
              case lit =>
                throw new ParseException(s"Invalid number of buckets: ${lit.describe}", applyCtx)
            }

            val fields = arguments.tail.map(arg => getFieldReference(applyCtx, arg))

            BucketTransform(LiteralValue(numBuckets, IntegerType), fields)

          case "years" =>
            YearsTransform(getSingleFieldReference(applyCtx, arguments))

          case "months" =>
            MonthsTransform(getSingleFieldReference(applyCtx, arguments))

          case "days" =>
            DaysTransform(getSingleFieldReference(applyCtx, arguments))

          case "hours" =>
            HoursTransform(getSingleFieldReference(applyCtx, arguments))

          case name =>
            ApplyTransform(name, arguments)
        }
    }
  }

  /**
   * Parse an argument to a transform. An argument may be a field reference (qualified name) or
   * a value literal.
   */
  override def visitTransformArgument(ctx: TransformArgumentContext): V2Expression = {
    withOrigin(ctx) {
      val reference = Option(ctx.qualifiedName)
        .map(typedVisit[Seq[String]])
        .map(FieldReference(_))
      val literal = Option(ctx.constant)
        .map(typedVisit[Literal])
        .map(lit => LiteralValue(lit.value, lit.dataType))
      reference.orElse(literal)
        .getOrElse(throw new ParseException("Invalid transform argument", ctx))
    }
  }

  def cleanTableProperties(
                            ctx: ParserRuleContext, properties: Map[String, String]): Map[String, String] = {
    import TableCatalog._
    val legacyOn = conf.getConf(SQLConf.LEGACY_PROPERTY_NON_RESERVED)
    properties.filter {
      case (PROP_PROVIDER, _) if !legacyOn =>
        throw new ParseException(s"$PROP_PROVIDER is a reserved table property, please use the USING clause to specify it.", ctx)
      case (PROP_PROVIDER, _) => false
      case (PROP_LOCATION, _) if !legacyOn =>
        throw new ParseException(s"$PROP_LOCATION is a reserved table property, please use the LOCATION clause to specify it.", ctx)
      case (PROP_LOCATION, _) => false
      case (PROP_OWNER, _) if !legacyOn =>
        throw new ParseException(s"$PROP_OWNER is a reserved table property, it will be set to the current user.", ctx)
      case (PROP_OWNER, _) => false
      case _ => true
    }
  }

  def cleanTableOptions(
                         ctx: ParserRuleContext,
                         options: Map[String, String],
                         location: Option[String]): (Map[String, String], Option[String]) = {
    var path = location
    val filtered = cleanTableProperties(ctx, options).filter {
      case (k, v) if k.equalsIgnoreCase("path") && path.nonEmpty =>
        throw new ParseException(s"Duplicated table paths found: '${path.get}' and '$v'. LOCATION" +
          s" and the case insensitive key 'path' in OPTIONS are all used to indicate the custom" +
          s" table path, you can only specify one of them.", ctx)
      case (k, v) if k.equalsIgnoreCase("path") =>
        path = Some(v)
        false
      case _ => true
    }
    (filtered, path)
  }

  /**
   * Create a [[SerdeInfo]] for creating tables.
   *
   * Format: STORED AS (name | INPUTFORMAT input_format OUTPUTFORMAT output_format)
   */
  override def visitCreateFileFormat(ctx: CreateFileFormatContext): SerdeInfo = withOrigin(ctx) {
    (ctx.fileFormat, ctx.storageHandler) match {
      // Expected format: INPUTFORMAT input_format OUTPUTFORMAT output_format
      case (c: TableFileFormatContext, null) =>
        SerdeInfo(formatClasses = Some(FormatClasses(string(c.inFmt), string(c.outFmt))))
      // Expected format: SEQUENCEFILE | TEXTFILE | RCFILE | ORC | PARQUET | AVRO
      case (c: GenericFileFormatContext, null) =>
        SerdeInfo(storedAs = Some(c.identifier.getText))
      case (null, storageHandler) =>
        operationNotAllowed("STORED BY", ctx)
      case _ =>
        throw new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
    }
  }

  /**
   * Create a [[SerdeInfo]] used for creating tables.
   *
   * Example format:
   * {{{
   *   SERDE serde_name [WITH SERDEPROPERTIES (k1=v1, k2=v2, ...)]
   * }}}
   *
   * OR
   *
   * {{{
   *   DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]]
   *   [COLLECTION ITEMS TERMINATED BY char]
   *   [MAP KEYS TERMINATED BY char]
   *   [LINES TERMINATED BY char]
   *   [NULL DEFINED AS char]
   * }}}
   */
  def visitRowFormat(ctx: RowFormatContext): SerdeInfo = withOrigin(ctx) {
    ctx match {
      case serde: RowFormatSerdeContext => visitRowFormatSerde(serde)
      case delimited: RowFormatDelimitedContext => visitRowFormatDelimited(delimited)
    }
  }

  /**
   * Create SERDE row format name and properties pair.
   */
  override def visitRowFormatSerde(ctx: RowFormatSerdeContext): SerdeInfo = withOrigin(ctx) {
    import ctx._
    SerdeInfo(
      serde = Some(string(name)),
      serdeProperties = Option(tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }

  /**
   * Create a delimited row format properties object.
   */
  override def visitRowFormatDelimited(
                                        ctx: RowFormatDelimitedContext): SerdeInfo = withOrigin(ctx) {
    // Collect the entries if any.
    def entry(key: String, value: Token): Seq[(String, String)] = {
      Option(value).toSeq.map(x => key -> string(x))
    }

    // TODO we need proper support for the NULL format.
    val entries =
      entry("field.delim", ctx.fieldsTerminatedBy) ++
        entry("serialization.format", ctx.fieldsTerminatedBy) ++
        entry("escape.delim", ctx.escapedBy) ++
        // The following typo is inherited from Hive...
        entry("colelction.delim", ctx.collectionItemsTerminatedBy) ++
        entry("mapkey.delim", ctx.keysTerminatedBy) ++
        Option(ctx.linesSeparatedBy).toSeq.map { token =>
          val value = string(token)
          validate(
            value == "\n",
            s"LINES TERMINATED BY only supports newline '\\n' right now: $value",
            ctx)
          "line.delim" -> value
        }
    SerdeInfo(serdeProperties = entries.toMap)
  }

  /**
   * Throw a [[ParseException]] if the user specified incompatible SerDes through ROW FORMAT
   * and STORED AS.
   *
   * The following are allowed. Anything else is not:
   * ROW FORMAT SERDE ... STORED AS [SEQUENCEFILE | RCFILE | TEXTFILE]
   * ROW FORMAT DELIMITED ... STORED AS TEXTFILE
   * ROW FORMAT ... STORED AS INPUTFORMAT ... OUTPUTFORMAT ...
   */
  protected def validateRowFormatFileFormat(
                                             rowFormatCtx: RowFormatContext,
                                             createFileFormatCtx: CreateFileFormatContext,
                                             parentCtx: ParserRuleContext): Unit = {
    if (!(rowFormatCtx == null || createFileFormatCtx == null)) {
      (rowFormatCtx, createFileFormatCtx.fileFormat) match {
        case (_, ffTable: TableFileFormatContext) => // OK
        case (rfSerde: RowFormatSerdeContext, ffGeneric: GenericFileFormatContext) =>
          ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
            case ("sequencefile" | "textfile" | "rcfile") => // OK
            case fmt =>
              operationNotAllowed(
                s"ROW FORMAT SERDE is incompatible with format '$fmt', which also specifies a serde",
                parentCtx)
          }
        case (rfDelimited: RowFormatDelimitedContext, ffGeneric: GenericFileFormatContext) =>
          ffGeneric.identifier.getText.toLowerCase(Locale.ROOT) match {
            case "textfile" => // OK
            case fmt => operationNotAllowed(
              s"ROW FORMAT DELIMITED is only compatible with 'textfile', not '$fmt'", parentCtx)
          }
        case _ =>
          // should never happen
          def str(ctx: ParserRuleContext): String = {
            (0 until ctx.getChildCount).map { i => ctx.getChild(i).getText }.mkString(" ")
          }

          operationNotAllowed(
            s"Unexpected combination of ${str(rowFormatCtx)} and ${str(createFileFormatCtx)}",
            parentCtx)
      }
    }
  }

  protected def validateRowFormatFileFormat(
                                             rowFormatCtx: Seq[RowFormatContext],
                                             createFileFormatCtx: Seq[CreateFileFormatContext],
                                             parentCtx: ParserRuleContext): Unit = {
    if (rowFormatCtx.size == 1 && createFileFormatCtx.size == 1) {
      validateRowFormatFileFormat(rowFormatCtx.head, createFileFormatCtx.head, parentCtx)
    }
  }

  override def visitCreateTableClauses(ctx: CreateTableClausesContext): TableClauses = {
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)
    checkDuplicateClauses(ctx.OPTIONS, "OPTIONS", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED BY", ctx)
    checkDuplicateClauses(ctx.createFileFormat, "STORED AS/BY", ctx)
    checkDuplicateClauses(ctx.rowFormat, "ROW FORMAT", ctx)
    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.bucketSpec(), "CLUSTERED BY", ctx)
    checkDuplicateClauses(ctx.locationSpec, "LOCATION", ctx)

    if (ctx.skewSpec.size > 0) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", ctx)
    }

    val (partTransforms, partCols) =
      Option(ctx.partitioning).map(visitPartitionFieldList).getOrElse((Nil, Nil))
    val bucketSpec = ctx.bucketSpec().asScala.headOption.map(visitBucketSpec)
    val properties = Option(ctx.tableProps).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val cleanedProperties = cleanTableProperties(ctx, properties)
    val options = Option(ctx.options).map(visitPropertyKeyValues).getOrElse(Map.empty)
    val location = visitLocationSpecList(ctx.locationSpec())
    val (cleanedOptions, newLocation) = cleanTableOptions(ctx, options, location)
    val comment = visitCommentSpecList(ctx.commentSpec())
    val serdeInfo =
      getSerdeInfo(ctx.rowFormat.asScala.toSeq, ctx.createFileFormat.asScala.toSeq, ctx)
    (partTransforms, partCols, bucketSpec, cleanedProperties, cleanedOptions, newLocation, comment,
      serdeInfo)
  }

  protected def getSerdeInfo(
                              rowFormatCtx: Seq[RowFormatContext],
                              createFileFormatCtx: Seq[CreateFileFormatContext],
                              ctx: ParserRuleContext): Option[SerdeInfo] = {
    validateRowFormatFileFormat(rowFormatCtx, createFileFormatCtx, ctx)
    val rowFormatSerdeInfo = rowFormatCtx.map(visitRowFormat)
    val fileFormatSerdeInfo = createFileFormatCtx.map(visitCreateFileFormat)
    (fileFormatSerdeInfo ++ rowFormatSerdeInfo).reduceLeftOption((l, r) => l.merge(r))
  }

  private def partitionExpressions(
                                    partTransforms: Seq[Transform],
                                    partCols: Seq[StructField],
                                    ctx: ParserRuleContext): Seq[Transform] = {
    if (partTransforms.nonEmpty) {
      if (partCols.nonEmpty) {
        val references = partTransforms.map(_.describe()).mkString(", ")
        val columns = partCols
          .map(field => s"${field.name} ${field.dataType.simpleString}")
          .mkString(", ")
        operationNotAllowed(
          s"""PARTITION BY: Cannot mix partition expressions and partition columns:
             |Expressions: $references
             |Columns: $columns""".stripMargin, ctx)

      }
      partTransforms
    } else {
      // columns were added to create the schema. convert to column references
      partCols.map { column =>
        IdentityTransform(FieldReference(Seq(column.name)))
      }
    }
  }

  /**
   * Create a table, returning a [[CreateTable]] or [[CreateTableAsSelect]] logical plan.
   *
   * Expected format:
   * {{{
   *   CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db_name.]table_name
   *   [USING table_provider]
   *   create_table_clauses
   *   [[AS] select_statement];
   *
   *   create_table_clauses (order insensitive):
   *     [PARTITIONED BY (partition_fields)]
   *     [OPTIONS table_property_list]
   *     [ROW FORMAT row_format]
   *     [STORED AS file_format]
   *     [CLUSTERED BY (col_name, col_name, ...)
   *       [SORTED BY (col_name [ASC|DESC], ...)]
   *       INTO num_buckets BUCKETS
   *     ]
   *     [LOCATION path]
   *     [COMMENT table_comment]
   *     [TBLPROPERTIES (property_name=property_value, ...)]
   *
   *   partition_fields:
   *     col_name, transform(col_name), transform(constant, col_name), ... |
   *     col_name data_type [NOT NULL] [COMMENT col_comment], ...
   * }}}
   */
  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = withOrigin(ctx) {
    val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)

    val columns = Option(ctx.colTypeList()).map(visitColTypeList).getOrElse(Nil)
    val provider = Option(ctx.tableProvider).map(_.multipartIdentifier.getText)
    val (partTransforms, partCols, bucketSpec, properties, options, location, comment, serdeInfo) =
      visitCreateTableClauses(ctx.createTableClauses())

    if (provider.isDefined && serdeInfo.isDefined) {
      operationNotAllowed(s"CREATE TABLE ... USING ... ${serdeInfo.get.describe}", ctx)
    }

    if (temp) {
      val asSelect = if (ctx.query == null) "" else " AS ..."
      operationNotAllowed(
        s"CREATE TEMPORARY TABLE ...$asSelect, use CREATE TEMPORARY VIEW instead", ctx)
    }

    // partition transforms for BucketSpec was moved inside parser
    // https://issues.apache.org/jira/browse/SPARK-37923
    val partitioning =
    partitionExpressions(partTransforms, partCols, ctx) ++ bucketSpec.map(_.asTransform)
    val tableSpec = TableSpec(properties, provider, options, location, comment,
      Option.empty, serdeInfo, external)

    Option(ctx.query).map(plan) match {
      case Some(_) if columns.nonEmpty =>
        operationNotAllowed(
          "Schema may not be specified in a Create Table As Select (CTAS) statement",
          ctx)

      case Some(_) if partCols.nonEmpty =>
        // non-reference partition columns are not allowed because schema can't be specified
        operationNotAllowed(
          "Partition column types may not be specified in Create Table As Select (CTAS)",
          ctx)

      // CreateTable / CreateTableAsSelect was migrated to v2 in Spark 3.3.0
      // https://issues.apache.org/jira/browse/SPARK-36850
      case Some(query) =>
        CreateTableAsSelect(
          UnresolvedIdentifier(table),
          partitioning, query, tableSpec, Map.empty, ifNotExists)

      case _ =>
        // Note: table schema includes both the table columns list and the partition columns
        // with data type.
        val schema = StructType(columns ++ partCols)
        CreateTable(
          UnresolvedIdentifier(table),
          schema.map(ColumnDefinition.fromV1Column(_, delegate)), partitioning, tableSpec, ignoreIfExists = ifNotExists)
    }
  }

  /**
   * Parse new column info from ADD COLUMN into a QualifiedColType.
   */
  override def visitQualifiedColTypeWithPosition(
                                                  ctx: QualifiedColTypeWithPositionContext): QualifiedColType = withOrigin(ctx) {
    val name = typedVisit[Seq[String]](ctx.name)
    QualifiedColType(
      path = if (name.length > 1) Some(UnresolvedFieldName(name.init)) else None,
      colName = name.last,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = ctx.NULL == null,
      comment = Option(ctx.commentSpec()).map(visitCommentSpec),
      position = Option(ctx.colPosition).map(pos =>
        UnresolvedFieldPosition(typedVisit[ColumnPosition](pos))),
      default = Option(null))
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

/**
 * A container for holding named common table expressions (CTEs) and a query plan.
 * This operator will be removed during analysis and the relations will be substituted into child.
 *
 * @param child        The final query of this CTE.
 * @param cteRelations A sequence of pair (alias, the CTE definition) that this CTE defined
 *                     Each CTE can see the base tables and the previously defined CTEs only.
 */
case class With(child: LogicalPlan, cteRelations: Seq[(String, SubqueryAlias)]) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def simpleString(maxFields: Int): String = {
    val cteAliases = truncatedString(cteRelations.map(_._1), "[", ", ", "]", maxFields)
    s"CTE $cteAliases"
  }

  override def innerChildren: Seq[LogicalPlan] = cteRelations.map(_._2)

  def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = this
}
