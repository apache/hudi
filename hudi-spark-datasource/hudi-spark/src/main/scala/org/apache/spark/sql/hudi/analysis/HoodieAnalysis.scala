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

import org.apache.hudi.common.util.ReflectionUtils
import org.apache.hudi.common.util.ReflectionUtils.loadClass
import org.apache.hudi.{HoodieSparkUtils, SparkAdapterSupport}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.{isMetaField, removeMetaFields}
import org.apache.spark.sql.hudi.analysis.HoodieAnalysis.{MatchCreateTableLike, MatchInsertIntoStatement, MatchMergeIntoTable, ResolvesToHudiTable, sparkAdapter}
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.command.procedures.{HoodieProcedures, Procedure, ProcedureArgs}
import org.apache.spark.sql.{AnalysisException, SparkSession}

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

    if (!HoodieSparkUtils.gteqSpark3_2) {
      //Add or correct resolution of MergeInto
      // the way we load the class via reflection is diff across spark2 and spark3 and hence had to split it out.
      if (HoodieSparkUtils.isSpark2) {
        val resolveReferencesClass = "org.apache.spark.sql.catalyst.analysis.HoodieSpark2Analysis$ResolveReferences"
        val sparkResolveReferences: RuleBuilder =
          session => ReflectionUtils.loadClass(resolveReferencesClass, session).asInstanceOf[Rule[LogicalPlan]]
        // TODO elaborate on the ordering
        rules += (adaptIngestionTargetLogicalRelations, sparkResolveReferences)
      } else if (HoodieSparkUtils.isSpark3_0) {
        val resolveReferencesClass = "org.apache.spark.sql.catalyst.analysis.HoodieSpark30Analysis$ResolveReferences"
        val sparkResolveReferences: RuleBuilder = {
          session => instantiateKlass(resolveReferencesClass, session)
        }
        // TODO elaborate on the ordering
        rules += (adaptIngestionTargetLogicalRelations, sparkResolveReferences)
      } else if (HoodieSparkUtils.isSpark3_1) {
        val resolveReferencesClass = "org.apache.spark.sql.catalyst.analysis.HoodieSpark31Analysis$ResolveReferences"
        val sparkResolveReferences: RuleBuilder =
          session => instantiateKlass(resolveReferencesClass, session)
        // TODO elaborate on the ordering
        rules += (adaptIngestionTargetLogicalRelations, sparkResolveReferences)
      } else {
        throw new IllegalStateException("Impossible to be here")
      }
    } else {
      rules += adaptIngestionTargetLogicalRelations
      val dataSourceV2ToV1FallbackClass = if (HoodieSparkUtils.isSpark3_5)
        "org.apache.spark.sql.hudi.analysis.HoodieSpark35DataSourceV2ToV1Fallback"
      else if (HoodieSparkUtils.isSpark3_4)
        "org.apache.spark.sql.hudi.analysis.HoodieSpark34DataSourceV2ToV1Fallback"
      else if (HoodieSparkUtils.isSpark3_3)
        "org.apache.spark.sql.hudi.analysis.HoodieSpark33DataSourceV2ToV1Fallback"
      else {
        // Spark 3.2.x
        "org.apache.spark.sql.hudi.analysis.HoodieSpark32DataSourceV2ToV1Fallback"
      }
      val dataSourceV2ToV1Fallback: RuleBuilder =
        session => instantiateKlass(dataSourceV2ToV1FallbackClass, session)

      val spark32PlusResolveReferencesClass = "org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusResolveReferences"
      val spark32PlusResolveReferences: RuleBuilder =
        session => instantiateKlass(spark32PlusResolveReferencesClass, session)

      // NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
      //
      // It's critical for this rules to follow in this order; re-ordering this rules might lead to changes in
      // behavior of Spark's analysis phase (for ex, DataSource V2 to V1 fallback might not kick in before other rules,
      // leading to all relations resolving as V2 instead of current expectation of them being resolved as V1)
      rules ++= Seq(dataSourceV2ToV1Fallback, spark32PlusResolveReferences)
    }

    if (HoodieSparkUtils.isSpark3) {
      val resolveAlterTableCommandsClass =
        if (HoodieSparkUtils.gteqSpark3_5) {
          "org.apache.spark.sql.hudi.Spark35ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_4) {
          "org.apache.spark.sql.hudi.Spark34ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_3) {
          "org.apache.spark.sql.hudi.Spark33ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_2) {
          "org.apache.spark.sql.hudi.Spark32ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_1) {
          "org.apache.spark.sql.hudi.Spark31ResolveHudiAlterTableCommand"
        } else if (HoodieSparkUtils.gteqSpark3_0) {
          "org.apache.spark.sql.hudi.Spark30ResolveHudiAlterTableCommand"
        } else {
          throw new IllegalStateException("Unsupported Spark version")
        }

      val resolveAlterTableCommands: RuleBuilder =
        session => instantiateKlass(resolveAlterTableCommandsClass, session)

      rules += resolveAlterTableCommands
    }

    // NOTE: Some of the conversions (for [[CreateTable]], [[InsertIntoStatement]] have to happen
    //       early to preempt execution of [[DataSourceAnalysis]] rule from Spark
    //       Please check rule's scala-doc for more details
    rules += (_ => ResolveImplementationsEarly())

    rules.toSeq
  }

  def customPostHocResolutionRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // NOTE: By default all commands are converted into corresponding Hudi implementations during
      //       "post-hoc resolution" phase
      session => ResolveImplementations(),
      session => HoodiePostAnalysisRule(session)
    )

    if (HoodieSparkUtils.gteqSpark3_2) {
      val spark3PostHocResolutionClass = "org.apache.spark.sql.hudi.analysis.HoodieSpark32PlusPostAnalysisRule"
      val spark3PostHocResolution: RuleBuilder =
        session => instantiateKlass(spark3PostHocResolutionClass, session)

      rules += spark3PostHocResolution
    }

    rules.toSeq
  }

  def customOptimizerRules: Seq[RuleBuilder] = {
    val rules: ListBuffer[RuleBuilder] = ListBuffer(
      // Default rules
    )

    if (HoodieSparkUtils.gteqSpark3_0) {
      val nestedSchemaPruningClass =
        if (HoodieSparkUtils.gteqSpark3_5) {
          "org.apache.spark.sql.execution.datasources.Spark35NestedSchemaPruning"
        } else if (HoodieSparkUtils.gteqSpark3_4) {
          "org.apache.spark.sql.execution.datasources.Spark34NestedSchemaPruning"
        } else if (HoodieSparkUtils.gteqSpark3_3) {
          "org.apache.spark.sql.execution.datasources.Spark33NestedSchemaPruning"
        } else if (HoodieSparkUtils.gteqSpark3_2) {
          "org.apache.spark.sql.execution.datasources.Spark32NestedSchemaPruning"
        } else if (HoodieSparkUtils.gteqSpark3_1) {
          // spark 3.1
          "org.apache.spark.sql.execution.datasources.Spark31NestedSchemaPruning"
        } else {
          // spark 3.0
          "org.apache.spark.sql.execution.datasources.Spark30NestedSchemaPruning"
        }

      val nestedSchemaPruningRule = ReflectionUtils.loadClass(nestedSchemaPruningClass).asInstanceOf[Rule[LogicalPlan]]
      rules += (_ => nestedSchemaPruningRule)
    }

    // NOTE: [[HoodiePruneFileSourcePartitions]] is a replica in kind to Spark's
    //       [[PruneFileSourcePartitions]] and as such should be executed at the same stage.
    //       However, currently Spark doesn't allow [[SparkSessionExtensions]] to inject into
    //       [[BaseSessionStateBuilder.customEarlyScanPushDownRules]] even though it could directly
    //       inject into the Spark's [[Optimizer]]
    //
    //       To work this around, we injecting this as the rule that trails pre-CBO, ie it's
    //          - Triggered before CBO, therefore have access to the same stats as CBO
    //          - Precedes actual [[customEarlyScanPushDownRules]] invocation
    rules += (spark => HoodiePruneFileSourcePartitions(spark))

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
              val mergeIntoTable = mit.asInstanceOf[MergeIntoTable]
              // Use all parameters to avoid NoSuchMethodError when method signature changes between Spark versions
              mergeIntoTable.copy(
                targetTable = updatedTargetTable.getOrElse(targetTable),
                sourceTable = updatedQuery.getOrElse(query),
                mergeCondition = mergeIntoTable.mergeCondition,
                matchedActions = mergeIntoTable.matchedActions,
                notMatchedActions = mergeIntoTable.notMatchedActions)
            } else {
              mit
            }

          // NOTE: In case of [[InsertIntoStatement]] Hudi tables could be on both sides -- receiving and providing
          //       the data, as such we have to make sure that we handle both of these cases
          case iis @ MatchInsertIntoStatement(targetTable, _, query, _, _) =>
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
            sparkAdapter.getCatalystPlanUtils.applyNewHoodieParquetFileFormatProjection(logicalPlan)
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
    def unapply(plan: LogicalPlan): Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] =
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

  private[sql] def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
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
case class ResolveImplementationsEarly() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Convert to InsertIntoHoodieTableCommand
      case iis @ MatchInsertIntoStatement(relation @ ResolvesToHudiTable(_), partition, query, overwrite, _) if query.resolved =>
        relation match {
          // NOTE: In Spark >= 3.2, Hudi relations will be resolved as [[DataSourceV2Relation]]s by default;
          //       However, currently, fallback will be applied downgrading them to V1 relations, hence
          //       we need to check whether we could proceed here, or has to wait until fallback rule kicks in
          case lr: LogicalRelation => new InsertIntoHoodieTableCommand(lr, query, partition, overwrite)
          case _ => iis
        }

      // Convert to CreateHoodieTableAsSelectCommand
      case ct @ CreateTable(table, mode, Some(query))
        if sparkAdapter.isHoodieTable(table) && ct.query.forall(_.resolved) =>
        CreateHoodieTableAsSelectCommand(table, mode, query)

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
case class ResolveImplementations() extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan match {
        // Convert to MergeIntoHoodieTableCommand
        case mit@MatchMergeIntoTable(target@ResolvesToHudiTable(_), _, _) if mit.resolved =>
          MergeIntoHoodieTableCommand(ReplaceExpressions(mit).asInstanceOf[MergeIntoTable])

        // Convert to UpdateHoodieTableCommand
        case ut@UpdateTable(plan@ResolvesToHudiTable(_), _, _) if ut.resolved =>
          UpdateHoodieTableCommand(ut)

        // Convert to DeleteHoodieTableCommand
        case dft@DeleteFromTable(plan@ResolvesToHudiTable(_), _) if dft.resolved =>
          DeleteHoodieTableCommand(dft)

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
        case ci @ CreateIndex(plan @ ResolvesToHudiTable(table), indexName, indexType, ignoreIfExists, columns, options, output) =>
          // TODO need to resolve columns
          CreateIndexCommand(table, indexName, indexType, ignoreIfExists, columns, options, output)

        // Convert to DropIndexCommand
        case di @ DropIndex(plan @ ResolvesToHudiTable(table), indexName, ignoreIfNotExists, output) if di.resolved =>
          DropIndexCommand(table, indexName, ignoreIfNotExists, output)

        // Convert to ShowIndexesCommand
        case si @ ShowIndexes(plan @ ResolvesToHudiTable(table), output) if si.resolved =>
          ShowIndexesCommand(table, output)

        // Covert to RefreshCommand
        case ri @ RefreshIndex(plan @ ResolvesToHudiTable(table), indexName, output) if ri.resolved =>
          RefreshIndexCommand(table, indexName, output)

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
        throw new AnalysisException(s"procedure: ${name.last} is not exists")
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
      // Rewrite the AlterTableDropPartitionCommand to AlterHoodieTableDropPartitionCommand
      case AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
        if sparkAdapter.isHoodieTable(tableName, sparkSession) =>
          AlterHoodieTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      // Rewrite the AlterTableAddColumnsCommand to AlterHoodieTableAddColumnsCommand
      case AlterTableAddColumnsCommand(tableId, colsToAdd)
        if sparkAdapter.isHoodieTable(tableId, sparkSession) =>
          AlterHoodieTableAddColumnsCommand(tableId, colsToAdd)
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
