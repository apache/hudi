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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.{HoodieSchemaUtils, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.common.util.{ReflectionUtils, ValidationUtils}
import org.apache.hudi.common.util.ReflectionUtils.loadClass

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HoodieCatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{isMetaField, removeMetaFields}
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.{sparkAdapter, MatchCreateIndex, MatchCreateTableLike, MatchDropIndex, MatchInsertIntoStatement, MatchMergeIntoTable, MatchRefreshIndex, MatchShowIndexes, ResolvesToHudiTable}
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.command.HoodieLeafRunnableCommand.stripMetaFieldAttributes
import org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand.alignQueryOutput
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedures, Procedure, ProcedureArgs}

import java.util

import scala.collection.mutable.ListBuffer

object HoodieAnalysis extends SparkAdapterSupport {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]

  def customResolutionRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer()

    // NOTE: This rule adjusts [[LogicalRelation]]s resolving into Hudi tables such that
    //       meta-fields are not affecting the resolution of the target columns to be updated by Spark (Except in the
    //       case of MergeInto. We leave the meta columns on the target table, and use other means to ensure resolution)
    //       For more details please check out the scala-doc of the rule
    val adaptIngestionTargetLogicalRelations: RuleBuilder = session => AdaptIngestionTargetLogicalRelations(session)

    rules += adaptIngestionTargetLogicalRelations
    val dataSourceV2ToV1FallbackClass = if (HoodieSparkUtils.isSpark4_0)
      "org.apache.spark.sql.hudi.analysis.HoodieSpark40DataSourceV2ToV1Fallback"
    else if (HoodieSparkUtils.isSpark3_5)
      "org.apache.spark.sql.hudi.analysis.HoodieSpark35DataSourceV2ToV1Fallback"
    else if (HoodieSparkUtils.isSpark3_4)
      "org.apache.spark.sql.hudi.analysis.HoodieSpark34DataSourceV2ToV1Fallback"
    else {
      // Spark 3.3.x
      "org.apache.spark.sql.hudi.analysis.HoodieSpark33DataSourceV2ToV1Fallback"
    }
    val dataSourceV2ToV1Fallback: RuleBuilder =
      session => instantiateKlass(dataSourceV2ToV1FallbackClass, session)

    val resolveReferencesClass = "org.apache.spark.sql.hudi.analysis.ResolveReferences"
    val resolveReferences: RuleBuilder =
      session => instantiateKlass(resolveReferencesClass, session)

    // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
    //
    // It's critical for this rules to follow in this order; re-ordering this rules might lead to changes in
    // behavior of Spark's analysis phase (for ex, DataSource V2 to V1 fallback might not kick in before other rules,
    // leading to all relations resolving as V2 instead of current expectation of them being resolved as V1)
    rules ++= Seq(dataSourceV2ToV1Fallback, resolveReferences)

    if (HoodieSparkUtils.isSpark3_5) {
      rules += (_ => instantiateKlass(
        "org.apache.spark.sql.hudi.analysis.HoodieSpark35ResolveColumnsForInsertInto"))
    }
    if (HoodieSparkUtils.isSpark4_0) {
      rules += (_ => instantiateKlass(
        "org.apache.spark.sql.hudi.analysis.HoodieSpark40ResolveColumnsForInsertInto"))
    }

    val resolveAlterTableCommandsClass =
      if (HoodieSparkUtils.gteqSpark4_0) {
        "org.apache.spark.sql.hudi.Spark40ResolveHudiAlterTableCommand"
      } else if (HoodieSparkUtils.gteqSpark3_5) {
        "org.apache.spark.sql.hudi.Spark35ResolveHudiAlterTableCommand"
      } else if (HoodieSparkUtils.isSpark3_4) {
        "org.apache.spark.sql.hudi.Spark34ResolveHudiAlterTableCommand"
      } else if (HoodieSparkUtils.isSpark3_3) {
        "org.apache.spark.sql.hudi.Spark33ResolveHudiAlterTableCommand"
      } else {
        throw new IllegalStateException("Unsupported Spark version")
      }

    val resolveAlterTableCommands: RuleBuilder =
      session => instantiateKlass(resolveAlterTableCommandsClass, session)

    rules += resolveAlterTableCommands

    // NOTE: Some of the conversions (for [[CreateTable]], [[InsertIntoStatement]] have to happen
    //       early to preempt execution of [[DataSourceAnalysis]] rule from Spark
    //       Please check rule's scala-doc for more details
    rules += (session => ResolveImplementationsEarly(session))

    rules.toSeq
  }

  def customPostHocResolutionRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // NOTE: By default all commands are converted into corresponding Hudi implementations during
      //       "post-hoc resolution" phase
      session => ResolveImplementations(session),
      session => HoodiePostAnalysisRule(session)
    )

    val sparkBasePostHocResolutionClass = "org.apache.spark.sql.hudi.analysis.HoodieSparkBasePostAnalysisRule"
    val sparkBasePostHocResolution: RuleBuilder =
      session => instantiateKlass(sparkBasePostHocResolutionClass, session)
    rules += sparkBasePostHocResolution

    rules.toSeq
  }

  def customOptimizerRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // Default rules
    )

    val nestedSchemaPruningClass = if (HoodieSparkUtils.gteqSpark4_0) {
        "org.apache.spark.sql.execution.datasources.Spark40NestedSchemaPruning"
      } else if (HoodieSparkUtils.gteqSpark3_5) {
        "org.apache.spark.sql.execution.datasources.Spark35NestedSchemaPruning"
      } else if (HoodieSparkUtils.gteqSpark3_4) {
        "org.apache.spark.sql.execution.datasources.Spark34NestedSchemaPruning"
      } else {
        // spark 3.3
        "org.apache.spark.sql.execution.datasources.Spark33NestedSchemaPruning"
      }

    val nestedSchemaPruningRule = ReflectionUtils.loadClass(nestedSchemaPruningClass).asInstanceOf[Rule[LogicalPlan]]
    rules += (_ => nestedSchemaPruningRule)

    // NOTE: [[HoodiePruneFileSourcePartitions]] is a replica in kind to Spark's
    //       [[PruneFileSourcePartitions]] and as such should be executed at the same stage.
    //       However, currently Spark doesn't allow [[SparkSessionExtensions]] to inject into
    //       [[BaseSessionStateBuilder.customEarlyScanPushDownRules]] even though it could directly
    //       inject into the Spark's [[Optimizer]]
    //
    //       To work this around, we injecting this as the rule that trails pre-CBO, ie it's
    //          - Triggered before CBO, therefore have access to the same stats as CBO
    //          - Precedes actual [[customEarlyScanPushDownRules]] invocation
    val hoodiePruneFileSourcePartitionsClass = if (HoodieSparkUtils.gteqSpark4_0) {
      "org.apache.spark.sql.hudi.analysis.Spark4HoodiePruneFileSourcePartitions"
    } else {
      "org.apache.spark.sql.hudi.analysis.Spark3HoodiePruneFileSourcePartitions"
    }
    rules += (spark => instantiateKlass(hoodiePruneFileSourcePartitionsClass, spark))

    rules.toSeq
  }

  /**
   * This rule adjusts output of the [[LogicalRelation]] resolving int Hudi tables such that all of the
   * default Spark resolution could be applied resolving standard Spark SQL commands
   *
   * <ul>
   *  <li>`MERGE INTO ...`</li>
   *  <li>`INSERT INTO ...`</li>
   *  <li>`UPDATE ...`</li>
   * </ul>
   *
   * even though Hudi tables might be carrying meta-fields that have to be ignored during resolution phase.
   *
   * Spark >= 3.2 bears fully-fledged support for meta-fields and such antics are not required for it:
   * we just need to annotate corresponding attributes as "metadata" for Spark to be able to ignore it.
   *
   * In Spark < 3.2 however, this is worked around by simply removing any meta-fields from the output
   * of the [[LogicalRelation]] resolving into Hudi table. Note that, it's a safe operation since we
   * actually need to ignore these values anyway
   */
  case class AdaptIngestionTargetLogicalRelations(spark: SparkSession) extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan =
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        plan transformDown {
          // NOTE: In case of [[MergeIntoTable]] Hudi tables could be on both sides -- receiving and providing
          //       the data, as such we have to make sure that we handle both of these cases
          case mit@MatchMergeIntoTable(targetTable, query, _) =>
            val updatedTargetTable = targetTable match {
              //Do not remove the meta cols here anymore
              case ResolvesToHudiTable(_) => Some(targetTable)
              case _ => None
            }

            val updatedQuery = query match {
              // In the producing side of the MIT, we simply check whether the query will be yielding
              // Hudi meta-fields attributes. In cases when it does we simply project them out
              //
              // NOTE: We have to handle both cases when [[query]] is fully resolved and when it's not,
              //       since, unfortunately, there's no reliable way for us to control the ordering of the
              //       application of the rules (during next iteration we might not even reach this rule again),
              //       therefore we have to make sure projection is handled in a single pass
              case ProducesHudiMetaFields(output) => Some(projectOutMetaFieldsAttributes(query, output))
              case _ => None
            }

            if (updatedTargetTable.isDefined || updatedQuery.isDefined) {
              mit.asInstanceOf[MergeIntoTable].copy(
                targetTable = updatedTargetTable.getOrElse(targetTable),
                sourceTable = updatedQuery.getOrElse(query)
              )
            } else {
              mit
            }

          // NOTE: In case of [[InsertIntoStatement]] Hudi tables could be on both sides -- receiving and providing
          //       the data, as such we have to make sure that we handle both of these cases
          case iis @ MatchInsertIntoStatement(targetTable, _, _, query, _, _) =>
            val updatedTargetTable = targetTable match {
              // In the receiving side of the IIS, we can't project meta-field attributes out,
              // and instead have to explicitly remove them
              case ResolvesToHudiTable(_) => Some(stripMetaFieldsAttributes(targetTable))
              case _ => None
            }

            val updatedQuery = query match {
              // In the producing side of the MIT, we simply check whether the query will be yielding
              // Hudi meta-fields attributes. In cases when it does we simply project them out
              //
              // NOTE: We have to handle both cases when [[query]] is fully resolved and when it's not,
              //       since, unfortunately, there's no reliable way for us to control the ordering of the
              //       application of the rules (during next iteration we might not even reach this rule again),
              //       therefore we have to make sure projection is handled in a single pass
              case ProducesHudiMetaFields(output) => Some(projectOutMetaFieldsAttributes(query, output))
              case _ => None
            }

            if (updatedTargetTable.isDefined || updatedQuery.isDefined) {
              sparkAdapter.getCatalystPlanUtils.rebaseInsertIntoStatement(iis,
                updatedTargetTable.getOrElse(targetTable), updatedQuery.getOrElse(query))
            } else {
              iis
            }

          case ut @ UpdateTable(relation @ ResolvesToHudiTable(_), _, _) =>
            ut.copy(table = relation)

          case logicalPlan: LogicalPlan if logicalPlan.resolved =>
            sparkAdapter.getCatalystPlanUtils.maybeApplyForNewFileFormat(logicalPlan)
        }
      }

    private def projectOutMetaFieldsAttributes(plan: LogicalPlan, output: Seq[Attribute]): LogicalPlan = {
      if (plan.resolved) {
        projectOutResolvedMetaFieldsAttributes(plan)
      } else {
        projectOutUnresolvedMetaFieldsAttributes(plan, output)
      }
    }

    private def projectOutUnresolvedMetaFieldsAttributes(plan: LogicalPlan, expected: Seq[Attribute]): LogicalPlan = {
      val filtered = expected.attrs.filterNot(attr => isMetaField(attr.name))
      if (filtered != expected) {
        Project(filtered.map(attr => UnresolvedAttribute(attr.name)), plan)
      } else {
        plan
      }
    }

    private def projectOutResolvedMetaFieldsAttributes(plan: LogicalPlan): LogicalPlan = {
      if (plan.output.exists(attr => isMetaField(attr.name))) {
        Project(removeMetaFields(plan.output), plan)
      } else {
        plan
      }
    }

    private def stripMetaFieldsAttributes(plan: LogicalPlan): LogicalPlan = {
      plan transformUp {
        case lr: LogicalRelation if lr.output.exists(attr => isMetaField(attr.name)) =>
          lr.copy(output = removeMetaFields(lr.output))
      }
    }

    private object ProducesHudiMetaFields {

      def unapply(plan: LogicalPlan): Option[Seq[Attribute]] = {
        val resolved = if (plan.resolved) {
          plan
        } else {
          val analyzer = spark.sessionState.analyzer
          analyzer.execute(plan)
        }

        if (resolved.output.exists(attr => isMetaField(attr.name))) {
          Some(resolved.output)
        } else {
          None
        }
      }
    }
  }

  private def instantiateKlass(klass: String): Rule[LogicalPlan] = {
    loadClass(klass).asInstanceOf[Rule[LogicalPlan]]
  }

  private def instantiateKlass(klass: String, session: SparkSession): Rule[LogicalPlan] = {
    // NOTE: We have to cast session to [[SparkSession]] sp that reflection lookup can
    //       find appropriate constructor in the target class
    loadClass(klass, Array(classOf[SparkSession]).asInstanceOf[Array[Class[_]]], session)
      .asInstanceOf[Rule[LogicalPlan]]
  }

  private[sql] object MatchMergeIntoTable {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] =
      sparkAdapter.getCatalystPlanUtils.unapplyMergeIntoTable(plan)
  }

  private[sql] object MatchInsertIntoStatement {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, Seq[String], Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] =
      sparkAdapter.getCatalystPlanUtils.unapplyInsertIntoStatement(plan)
  }

  private[sql] object ResolvesToHudiTable {
    def unapply(plan: LogicalPlan): Option[CatalogTable] =
      sparkAdapter.resolveHoodieTable(plan)
  }

  private[sql] object MatchCreateTableLike {
    def unapply(plan: LogicalPlan): Option[(TableIdentifier, TableIdentifier, CatalogStorageFormat, Option[String], Map[String, String], Boolean)] =
      sparkAdapter.getCatalystPlanUtils.unapplyCreateTableLikeCommand(plan)
  }

  private[sql] object MatchCreateIndex {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, String, String, Boolean, Seq[(Seq[String], Map[String, String])], Map[String, String])] =
      sparkAdapter.getCatalystPlanUtils.unapplyCreateIndex(plan)
  }

  private[sql] object MatchDropIndex {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, String, Boolean)] =
      sparkAdapter.getCatalystPlanUtils.unapplyDropIndex(plan)
  }

  private[sql] object MatchShowIndexes {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, Seq[Attribute])] =
      sparkAdapter.getCatalystPlanUtils.unapplyShowIndexes(plan)
  }

  private[sql] object MatchRefreshIndex {
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, String)] =
      sparkAdapter.getCatalystPlanUtils.unapplyRefreshIndex(plan)
  }

  private[sql] def failAnalysis(msg: String): Nothing = {
    throw new HoodieAnalysisException(msg)
  }
}

/**
 * Rule converting *fully-resolved* Spark SQL plans into Hudi's custom implementations
 *
 * NOTE: This is separated out from [[ResolveImplementations]] such that we can apply it
 *       during earlier stage (resolution), while the [[ResolveImplementations]] is applied at post-hoc
 *       resolution phase. This is necessary to make sure that [[ResolveImplementationsEarly]] preempts
 *       execution of the [[DataSourceAnalysis]] stage from Spark which would otherwise convert same commands
 *       into native Spark implementations (which are not compatible w/ Hudi)
 */
case class ResolveImplementationsEarly(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Convert to InsertIntoHoodieTableCommand
      case iis @ MatchInsertIntoStatement(relation @ ResolvesToHudiTable(_), userSpecifiedCols, partition, query, overwrite, _) if query.resolved =>
        relation match {
          // NOTE: In Spark >= 3.2, Hudi relations will be resolved as [[DataSourceV2Relation]]s by default;
          //       However, currently, fallback will be applied downgrading them to V1 relations, hence
          //       we need to check whether we could proceed here, or has to wait until fallback rule kicks in
          case lr: LogicalRelation =>
            // Create a project if this is an INSERT INTO query with specified cols.
            val projectByUserSpecified = if (userSpecifiedCols.nonEmpty) {
              ValidationUtils.checkState(lr.catalogTable.isDefined, "Missing catalog table")
              sparkAdapter.getCatalystPlanUtils.createProjectForByNameQuery(lr, iis)
            } else {
              None
            }
            val hoodieCatalogTable = new HoodieCatalogTable(spark, lr.catalogTable.get)
            val alignedQuery = alignQueryOutput(projectByUserSpecified.getOrElse(query), hoodieCatalogTable, partition, spark.sessionState.conf)
            new InsertIntoHoodieTableCommand(lr, alignedQuery, partition, overwrite)
          case _ => iis
        }

      // Convert to CreateHoodieTableAsSelectCommand
      case ct @ CreateTable(table, mode, Some(query))
        if sparkAdapter.isHoodieTable(table) && ct.query.forall(_.resolved) =>
        val alignedQuery = stripMetaFieldAttributes(query)
        CreateHoodieTableAsSelectCommand(table, mode, alignedQuery)

      case ct: CreateTable =>
        try {
          // NOTE: In case of CreateTable with schema and multiple partition fields,
          // we have to make sure that partition fields are ordered in the same way as they are in the schema.
          val tableSchema = ct.query.map(_.schema).getOrElse(ct.tableDesc.schema)
          HoodieSchemaUtils.checkPartitionSchemaOrder(tableSchema, ct.tableDesc.partitionColumnNames)
        } catch {
          case e: IllegalArgumentException =>
            throw e
          case _: Exception =>
            // NOTE: This case is when query is unresolved but table is a managed table and already exists.
            // In this case, create table will fail post-analysis (see [[HoodieCatalogTable.parseSchemaAndConfigs]]).
            logWarning("An unexpected exception occurred while checking partition schema order. Proceeding with the plan.")
        }
        plan

      case _ => plan
    }
  }
}

/**
 * Rule converting *fully-resolved* Spark SQL plans into Hudi's custom implementations
 *
 * NOTE: This is executed in "post-hoc resolution" phase to make sure all of the commands have
 *       been resolved prior to that
 */
case class ResolveImplementations(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan match {
        // Convert to MergeIntoHoodieTableCommand
        case mit@MatchMergeIntoTable(target@ResolvesToHudiTable(table), _, _) if mit.resolved =>
          val catalogTable = HoodieCatalogTable(sparkSession, table)
          val command = new MergeIntoHoodieTableCommand(ReplaceExpressions(mit).asInstanceOf[MergeIntoTable], catalogTable, sparkSession, null)
          val inputPlan = command.getProcessedInputPlan
          command.copy(query = inputPlan)

        // Convert to UpdateHoodieTableCommand
        case ut@UpdateTable(plan@ResolvesToHudiTable(_), _, _) if ut.resolved =>
          val inputPlan = UpdateHoodieTableCommand.inputPlan(sparkSession, ut)
          UpdateHoodieTableCommand(ut, inputPlan)


        // Convert to DeleteHoodieTableCommand
        case dft@DeleteFromTable(ResolvesToHudiTable(table), _) if dft.resolved =>
          val catalogTable = new HoodieCatalogTable(sparkSession, table)
          val (plan, config) = DeleteHoodieTableCommand.inputPlan(sparkSession, dft, catalogTable)
          DeleteHoodieTableCommand(catalogTable, plan, config)

        // Convert to CompactionHoodieTableCommand
        case ct @ CompactionTable(plan @ ResolvesToHudiTable(table), operation, options) if ct.resolved =>
          CompactionHoodieTableCommand(table, operation, options)

        // Convert to CompactionHoodiePathCommand
        case cp @ CompactionPath(path, operation, options) if cp.resolved =>
          CompactionHoodiePathCommand(path, operation, options)

        // Convert to CompactionShowOnTable
        case csot @ CompactionShowOnTable(plan @ ResolvesToHudiTable(table), limit) if csot.resolved =>
          CompactionShowHoodieTableCommand(table, limit)

        // Convert to CompactionShowHoodiePathCommand
        case csop @ CompactionShowOnPath(path, limit) if csop.resolved =>
          CompactionShowHoodiePathCommand(path, limit)

        // Convert to HoodieCallProcedureCommand
        case c @ CallCommand(_, _) =>
          val procedure: Option[Procedure] = loadProcedure(c.name)
          val input = buildProcedureArgs(c.args)
          if (procedure.nonEmpty) {
            CallProcedureHoodieCommand(procedure.get, input)
          } else {
            c
          }

        // Convert to CreateIndexCommand
        case ci @ MatchCreateIndex(plan @ ResolvesToHudiTable(table), indexName, indexType, ignoreIfExists, columns, options) if ci.resolved =>
          CreateIndexCommand(table, indexName, indexType, ignoreIfExists, columns, options)

        // Convert to DropIndexCommand
        case di @ MatchDropIndex(plan @ ResolvesToHudiTable(table), indexName, ignoreIfNotExists) if di.resolved =>
          DropIndexCommand(table, indexName, ignoreIfNotExists)

        // Convert to ShowIndexesCommand
        case si @ MatchShowIndexes(plan @ ResolvesToHudiTable(table), output) if si.resolved =>
          ShowIndexesCommand(table, output)

        // Covert to RefreshCommand
        case ri @ MatchRefreshIndex(plan @ ResolvesToHudiTable(table), indexName) if ri.resolved =>
          RefreshIndexCommand(table, indexName)

        case _ => plan
      }
    }
  }

  private def loadProcedure(name: Seq[String]): Option[Procedure] = {
    val procedure: Option[Procedure] = if (name.nonEmpty) {
      val builder = HoodieProcedures.newBuilder(name.last)
      if (builder != null) {
        Option(builder.build)
      } else {
        throw new HoodieAnalysisException(s"procedure: ${name.last} is not exists")
      }
    } else {
      None
    }
    procedure
  }

  private def buildProcedureArgs(exprs: Seq[CallArgument]): ProcedureArgs = {
    val values = new Array[Any](exprs.size)
    var isNamedArgs: Boolean = false
    val map = new util.LinkedHashMap[String, Int]()
    for (index <- exprs.indices) {
      exprs(index) match {
        case expr: NamedArgument =>
          map.put(expr.name, index)
          values(index) = expr.expr.eval()
          isNamedArgs = true
        case _ =>
          map.put(index.toString, index)
          values(index) = exprs(index).expr.eval()
          isNamedArgs = false
      }
    }
    ProcedureArgs(isNamedArgs, map, new GenericInternalRow(values))
  }
}

/**
 * Rule for rewrite some spark commands to hudi's implementation.
 * @param sparkSession
 *
 * TODO merge w/ ResolveImplementations
 */
case class HoodiePostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Rewrite the CreateDataSourceTableCommand to CreateHoodieTableCommand
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if sparkAdapter.isHoodieTable(table) =>
        CreateHoodieTableCommand(table, ignoreIfExists)
      case MatchCreateTableLike(targetTable, sourceTable, fileFormat, provider, properties, ifNotExists)
        if sparkAdapter.isHoodieTable(provider.orNull) =>
        CreateHoodieTableLikeCommand(targetTable, sourceTable, fileFormat, properties, ifNotExists)
      // Rewrite the DropTableCommand to DropHoodieTableCommand
      case DropTableCommand(tableName, ifExists, false, purge)
        if sparkSession.sessionState.catalog.tableExists(tableName)
          && sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        DropHoodieTableCommand(tableName, ifExists, false, purge)
      // Rewrite the AlterTableAddPartitionCommand to AlterHoodieTableAddPartitionCommand
      case AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        AlterHoodieTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists)
      // Rewrite the AlterTableDropPartitionCommand to AlterHoodieTableDropPartitionCommand
      case AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
          AlterHoodieTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
      // Rewrite the AlterTableAddColumnsCommand to AlterHoodieTableAddColumnsCommand
      case AlterTableAddColumnsCommand(tableId, colsToAdd)
        if sparkAdapter.isHoodieTable(tableId, sparkSession) =>
          AlterHoodieTableAddColumnsCommand(tableId, colsToAdd)
      case s: ShowCreateTableCommand
        if sparkAdapter.isHoodieTable(s.table, sparkSession) =>
        ShowHoodieCreateTableCommand(s.table)
      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      case AlterTableRenameCommand(oldName, newName, isView)
        if !isView && sparkAdapter.isHoodieTable(oldName, sparkSession) =>
          AlterHoodieTableRenameCommand(oldName, newName, isView)
      // Rewrite the AlterTableChangeColumnCommand to AlterHoodieTableChangeColumnCommand
      case AlterTableChangeColumnCommand(tableName, columnName, newColumn)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
          AlterHoodieTableChangeColumnCommand(tableName, columnName, newColumn)
      // SPARK-34238: the definition of ShowPartitionsCommand has been changed in Spark3.2.
      // Match the class type instead of call the `unapply` method.
      case s: ShowPartitionsCommand
        if sparkAdapter.isHoodieTable(s.tableName, sparkSession) =>
          ShowHoodieTablePartitionsCommand(s.tableName, s.spec)
      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTableCommand(tableName, partitionSpec)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
        TruncateHoodieTableCommand(tableName, partitionSpec)
      // Rewrite RepairTableCommand to RepairHoodieTableCommand
      case r if sparkAdapter.getCatalystPlanUtils.isRepairTable(r) =>
        val (tableName, enableAddPartitions, enableDropPartitions, cmd) = sparkAdapter.getCatalystPlanUtils.getRepairTableChildren(r).get
        if (sparkAdapter.isHoodieTable(tableName, sparkSession)) {
          RepairHoodieTableCommand(tableName, enableAddPartitions, enableDropPartitions, cmd)
        } else {
          r
        }
      case _ => plan
    }
  }
}
