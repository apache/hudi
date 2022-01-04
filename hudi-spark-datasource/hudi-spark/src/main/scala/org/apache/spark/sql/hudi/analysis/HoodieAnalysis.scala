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

import org.apache.hudi.{HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.command._
import org.apache.spark.sql.hudi.{HoodieOptionConfig, HoodieSqlUtils}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.collection.JavaConverters._

object HoodieAnalysis {
  def customResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => HoodieResolveReferences(session),
      session => HoodieAnalysis(session)
    )

  def customPostHocResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => HoodiePostAnalysisRule(session)
    )
}

/**
 * Rule for convert the logical plan to command.
 * @param sparkSession
 */
case class HoodieAnalysis(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Convert to MergeIntoHoodieTableCommand
      case m @ MergeIntoTable(target, _, _, _, _)
        if m.resolved && isHoodieTable(target, sparkSession) =>
          MergeIntoHoodieTableCommand(m)

      // Convert to UpdateHoodieTableCommand
      case u @ UpdateTable(table, _, _)
        if u.resolved && isHoodieTable(table, sparkSession) =>
          UpdateHoodieTableCommand(u)

      // Convert to DeleteHoodieTableCommand
      case d @ DeleteFromTable(table, _)
        if d.resolved && isHoodieTable(table, sparkSession) =>
          DeleteHoodieTableCommand(d)

      // Convert to InsertIntoHoodieTableCommand
      case l if sparkAdapter.isInsertInto(l) =>
        val (table, partition, query, overwrite, _) = sparkAdapter.getInsertIntoChildren(l).get
        table match {
          case relation: LogicalRelation if isHoodieTable(relation, sparkSession) =>
            new InsertIntoHoodieTableCommand(relation, query, partition, overwrite)
          case _ =>
            l
        }
      // Convert to CreateHoodieTableAsSelectCommand
      case CreateTable(table, mode, Some(query))
        if query.resolved && isHoodieTable(table) =>
          CreateHoodieTableAsSelectCommand(table, mode, query)

      // Convert to CompactionHoodieTableCommand
      case CompactionTable(table, operation, options)
        if table.resolved && isHoodieTable(table, sparkSession) =>
        val tableId = getTableIdentify(table)
        val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
        CompactionHoodieTableCommand(catalogTable, operation, options)
      // Convert to CompactionHoodiePathCommand
      case CompactionPath(path, operation, options) =>
        CompactionHoodiePathCommand(path, operation, options)
      // Convert to CompactionShowOnTable
      case CompactionShowOnTable(table, limit)
        if isHoodieTable(table, sparkSession) =>
        val tableId = getTableIdentify(table)
        val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
        CompactionShowHoodieTableCommand(catalogTable, limit)
      // Convert to CompactionShowHoodiePathCommand
      case CompactionShowOnPath(path, limit) =>
        CompactionShowHoodiePathCommand(path, limit)
      case _=> plan
    }
  }
}

/**
 * Rule for resolve hoodie's extended syntax or rewrite some logical plan.
 * @param sparkSession
 */
case class HoodieResolveReferences(sparkSession: SparkSession) extends Rule[LogicalPlan]
  with SparkAdapterSupport {
  private lazy val analyzer = sparkSession.sessionState.analyzer

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp  {
    // Resolve merge into
    case mergeInto @ MergeIntoTable(target, source, mergeCondition, matchedActions, notMatchedActions)
      if isHoodieTable(target, sparkSession) && target.resolved =>

      val resolver = sparkSession.sessionState.conf.resolver
      val resolvedSource = analyzer.execute(source)
      def isInsertOrUpdateStar(assignments: Seq[Assignment]): Boolean = {
        if (assignments.isEmpty) {
          true
        } else {
          // This is a Hack for test if it is "update set *" or "insert *" for spark3.
          // As spark3's own ResolveReference will append first five columns of the target
          // table(which is the hoodie meta fields) to the assignments for "update set *" and
          // "insert *", so we test if the first five assignmentFieldNames is the meta fields
          // to judge if it is "update set *" or "insert *".
          // We can do this because under the normal case, we should not allow to update or set
          // the hoodie's meta field in sql statement, it is a system field, cannot set the value
          // by user.
          if (HoodieSparkUtils.isSpark3) {
            val assignmentFieldNames = assignments.map(_.key).map {
              case attr: AttributeReference =>
                attr.name
              case _ => ""
            }.toArray
            val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
            if (metaFields.mkString(",").startsWith(assignmentFieldNames.take(metaFields.length).mkString(","))) {
              true
            } else {
              false
            }
          } else {
            false
          }
        }
      }

      def resolveConditionAssignments(condition: Option[Expression],
        assignments: Seq[Assignment]): (Option[Expression], Seq[Assignment]) = {
        val resolvedCondition = condition.map(resolveExpressionFrom(resolvedSource)(_))
        val resolvedAssignments = if (isInsertOrUpdateStar(assignments)) {
          // assignments is empty means insert * or update set *
          val resolvedSourceOutput = resolvedSource.output.filter(attr => !HoodieSqlUtils.isMetaField(attr.name))
          val targetOutput = target.output.filter(attr => !HoodieSqlUtils.isMetaField(attr.name))
          val resolvedSourceColumnNames = resolvedSourceOutput.map(_.name)

          if(targetOutput.filter(attr => resolvedSourceColumnNames.exists(resolver(_, attr.name))).equals(targetOutput)){
            //If sourceTable's columns contains all targetTable's columns,
            //We fill assign all the source fields to the target fields by column name matching.
            targetOutput.map(targetAttr => {
              val sourceAttr = resolvedSourceOutput.find(f => resolver(f.name, targetAttr.name)).get
              Assignment(targetAttr, sourceAttr)
            })
          } else {
            // We fill assign all the source fields to the target fields by order.
            targetOutput
              .zip(resolvedSourceOutput)
              .map { case (targetAttr, sourceAttr) => Assignment(targetAttr, sourceAttr) }
          }
        } else {
          // For Spark3.2, InsertStarAction/UpdateStarAction's assignments will contain the meta fields.
          val withoutMetaAttrs = assignments.filterNot{ assignment =>
            if (assignment.key.isInstanceOf[Attribute]) {
              HoodieSqlUtils.isMetaField(assignment.key.asInstanceOf[Attribute].name)
            } else {
              false
            }
          }
          withoutMetaAttrs.map { assignment =>
            val resolvedKey = resolveExpressionFrom(target)(assignment.key)
            val resolvedValue = resolveExpressionFrom(resolvedSource, Some(target))(assignment.value)
            Assignment(resolvedKey, resolvedValue)
          }
        }
        (resolvedCondition, resolvedAssignments)
      }

      // Resolve the merge condition
      val resolvedMergeCondition = resolveExpressionFrom(resolvedSource, Some(target))(mergeCondition)

      // Resolve the matchedActions
      val resolvedMatchedActions = matchedActions.map {
        case UpdateAction(condition, assignments) =>
          val (resolvedCondition, resolvedAssignments) =
            resolveConditionAssignments(condition, assignments)

          // Get the target table type and pre-combine field.
          val targetTableId = getMergeIntoTargetTableId(mergeInto)
          val targetTable =
            sparkSession.sessionState.catalog.getTableMetadata(targetTableId)
          val tblProperties = targetTable.storage.properties ++ targetTable.properties
          val targetTableType = HoodieOptionConfig.getTableType(tblProperties)
          val preCombineField = HoodieOptionConfig.getPreCombineField(tblProperties)

          // Get the map of target attribute to value of the update assignments.
          val target2Values = resolvedAssignments.map {
              case Assignment(attr: AttributeReference, value) =>
                attr.name -> value
              case o => throw new IllegalArgumentException(s"Assignment key must be an attribute, current is: ${o.key}")
          }.toMap

          // Validate if there are incorrect target attributes.
          val targetColumnNames = removeMetaFields(target.output).map(_.name)
          val unKnowTargets = target2Values.keys
            .filterNot(name => targetColumnNames.exists(resolver(_, name)))
          if (unKnowTargets.nonEmpty) {
            throw new AnalysisException(s"Cannot find target attributes: ${unKnowTargets.mkString(",")}.")
          }

          // Fill the missing target attribute in the update action for COW table to support partial update.
          // e.g. If the update action missing 'id' attribute, we fill a "id = target.id" to the update action.
          val newAssignments = removeMetaFields(target.output)
            .map(attr => {
              val valueOption = target2Values.find(f => resolver(f._1, attr.name))
              // TODO support partial update for MOR.
              if (valueOption.isEmpty && targetTableType == MOR_TABLE_TYPE_OPT_VAL) {
                throw new AnalysisException(s"Missing specify the value for target field: '${attr.name}' in merge into update action" +
                  s" for MOR table. Currently we cannot support partial update for MOR," +
                  s" please complete all the target fields just like '...update set id = s0.id, name = s0.name ....'")
              }
              if (preCombineField.isDefined && preCombineField.get.equalsIgnoreCase(attr.name)
                  && valueOption.isEmpty) {
                throw new AnalysisException(s"Missing specify value for the preCombineField:" +
                  s" ${preCombineField.get} in merge-into update action. You should add" +
                  s" '... update set ${preCombineField.get} = xx....' to the when-matched clause.")
              }
              Assignment(attr, if (valueOption.isEmpty) attr else valueOption.get._2)
            })
          UpdateAction(resolvedCondition, newAssignments)
        case DeleteAction(condition) =>
          val resolvedCondition = condition.map(resolveExpressionFrom(resolvedSource)(_))
          DeleteAction(resolvedCondition)
        case action: MergeAction =>
          // SPARK-34962:  use UpdateStarAction as the explicit representation of * in UpdateAction.
          // So match and covert this in Spark3.2 env.
          val (resolvedCondition, resolvedAssignments) =
            resolveConditionAssignments(action.condition, Seq.empty)
          UpdateAction(resolvedCondition, resolvedAssignments)
      }
      // Resolve the notMatchedActions
      val resolvedNotMatchedActions = notMatchedActions.map {
        case InsertAction(condition, assignments) =>
          val (resolvedCondition, resolvedAssignments) =
            resolveConditionAssignments(condition, assignments)
          InsertAction(resolvedCondition, resolvedAssignments)
        case action: MergeAction =>
          // SPARK-34962:  use InsertStarAction as the explicit representation of * in InsertAction.
          // So match and covert this in Spark3.2 env.
          val (resolvedCondition, resolvedAssignments) =
            resolveConditionAssignments(action.condition, Seq.empty)
          InsertAction(resolvedCondition, resolvedAssignments)
      }
      // Return the resolved MergeIntoTable
      MergeIntoTable(target, resolvedSource, resolvedMergeCondition,
        resolvedMatchedActions, resolvedNotMatchedActions)

    // Resolve update table
    case UpdateTable(table, assignments, condition)
      if isHoodieTable(table, sparkSession) && table.resolved =>
      // Resolve condition
      val resolvedCondition = condition.map(resolveExpressionFrom(table)(_))
      // Resolve assignments
      val resolvedAssignments = assignments.map(assignment => {
        val resolvedKey = resolveExpressionFrom(table)(assignment.key)
        val resolvedValue = resolveExpressionFrom(table)(assignment.value)
          Assignment(resolvedKey, resolvedValue)
      })
      // Return the resolved UpdateTable
      UpdateTable(table, resolvedAssignments, resolvedCondition)

    // Resolve Delete Table
    case DeleteFromTable(table, condition)
      if isHoodieTable(table, sparkSession) && table.resolved =>
      // Resolve condition
      val resolvedCondition = condition.map(resolveExpressionFrom(table)(_))
      // Return the resolved DeleteTable
      DeleteFromTable(table, resolvedCondition)

    // Append the meta field to the insert query to walk through the validate for the
    // number of insert fields with the number of the target table fields.
    case l if sparkAdapter.isInsertInto(l) =>
      val (table, partition, query, overwrite, ifPartitionNotExists) =
        sparkAdapter.getInsertIntoChildren(l).get

      if (isHoodieTable(table, sparkSession) && query.resolved &&
        !containUnResolvedStar(query) &&
        !checkAlreadyAppendMetaField(query)) {
        val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.map(
          Alias(Literal.create(null, StringType), _)()).toArray[NamedExpression]
        val newQuery = query match {
          case project: Project =>
            val withMetaFieldProjects =
              metaFields ++ project.projectList
            // Append the meta fields to the insert query.
            Project(withMetaFieldProjects, project.child)
          case _ =>
            val withMetaFieldProjects = metaFields ++ query.output
            Project(withMetaFieldProjects, query)
        }
        sparkAdapter.createInsertInto(table, partition, newQuery, overwrite, ifPartitionNotExists)
      } else {
        l
      }
    // Fill schema for Create Table without specify schema info
    case c @ CreateTable(tableDesc, _, _)
      if isHoodieTable(tableDesc) =>
        val tablePath = getTableLocation(c.tableDesc, sparkSession)
        val tableExistInCatalog = sparkSession.sessionState.catalog.tableExists(tableDesc.identifier)
        // Only when the table has not exist in catalog, we need to fill the schema info for creating table.
        if (!tableExistInCatalog && tableExistsInPath(tablePath, sparkSession.sessionState.newHadoopConf())) {
          val metaClient = HoodieTableMetaClient.builder()
            .setBasePath(tablePath)
            .setConf(sparkSession.sessionState.newHadoopConf())
            .build()
          val tableSchema = HoodieSqlUtils.getTableSqlSchema(metaClient)
          if (tableSchema.isDefined && tableDesc.schema.isEmpty) {
            // Fill the schema with the schema from the table
            c.copy(tableDesc.copy(schema = tableSchema.get))
          } else if (tableSchema.isDefined && tableDesc.schema != tableSchema.get) {
            throw new AnalysisException(s"Specified schema in create table statement is not equal to the table schema." +
              s"You should not specify the schema for an exist table: ${tableDesc.identifier} ")
          } else {
            c
          }
        } else {
          c
        }
    case p => p
  }

  private def containUnResolvedStar(query: LogicalPlan): Boolean = {
    query match {
      case project: Project => project.projectList.exists(_.isInstanceOf[UnresolvedStar])
      case _ => false
    }
  }

  /**
   * Check if the the query of insert statement has already append the meta fields to avoid
   * duplicate append.
   * @param query
   * @return
   */
  private def checkAlreadyAppendMetaField(query: LogicalPlan): Boolean = {
    query.output.take(HoodieRecord.HOODIE_META_COLUMNS.size())
      .filter(isMetaField)
      .map {
        case AttributeReference(name, _, _, _) => name.toLowerCase
        case other => throw new IllegalArgumentException(s"$other should not be a hoodie meta field")
      }.toSet == HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet
  }

  private def isMetaField(exp: Expression): Boolean = {
    val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet
    exp match {
      case Alias(_, name) if metaFields.contains(name.toLowerCase) => true
      case AttributeReference(name, _, _, _) if metaFields.contains(name.toLowerCase) => true
      case _=> false
    }
  }

  /**
   * Resolve the expression.
   * 1、 Fake a a project for the expression based on the source plan
   * 2、 Resolve the fake project
   * 3、 Get the resolved expression from the faked project
   * @param left The left source plan for the expression.
   * @param right The right source plan for the expression.
   * @param expression The expression to resolved.
   * @return The resolved expression.
   */
  private def resolveExpressionFrom(left: LogicalPlan, right: Option[LogicalPlan] = None)
                        (expression: Expression): Expression = {
    // Fake a project for the expression based on the source plan.
    val fakeProject = if (right.isDefined) {
      Project(Seq(Alias(expression, "_c0")()),
        sparkAdapter.createJoin(left, right.get, Inner))
    } else {
      Project(Seq(Alias(expression, "_c0")()),
        left)
    }
    // Resolve the fake project
    val resolvedProject =
      analyzer.ResolveReferences.apply(fakeProject).asInstanceOf[Project]
    val unResolvedAttrs = resolvedProject.projectList.head.collect {
      case attr: UnresolvedAttribute => attr
    }
    if (unResolvedAttrs.nonEmpty) {
      throw new AnalysisException(s"Cannot resolve ${unResolvedAttrs.mkString(",")} in " +
        s"${expression.sql}, the input " + s"columns is: [${fakeProject.child.output.mkString(", ")}]")
    }
    // Fetch the resolved expression from the fake project.
    resolvedProject.projectList.head.asInstanceOf[Alias].child
  }
}

/**
 * Rule for rewrite some spark commands to hudi's implementation.
 * @param sparkSession
 */
case class HoodiePostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Rewrite the CreateDataSourceTableCommand to CreateHoodieTableCommand
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if isHoodieTable(table) =>
        CreateHoodieTableCommand(table, ignoreIfExists)
      // Rewrite the DropTableCommand to DropHoodieTableCommand
      case DropTableCommand(tableName, ifExists, isView, purge)
        if isHoodieTable(tableName, sparkSession) =>
        DropHoodieTableCommand(tableName, ifExists, isView, purge)
      // Rewrite the AlterTableDropPartitionCommand to AlterHoodieTableDropPartitionCommand
      case AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
        if isHoodieTable(tableName, sparkSession) =>
          AlterHoodieTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      // Rewrite the AlterTableAddColumnsCommand to AlterHoodieTableAddColumnsCommand
      case AlterTableAddColumnsCommand(tableId, colsToAdd)
        if isHoodieTable(tableId, sparkSession) =>
        AlterHoodieTableAddColumnsCommand(tableId, colsToAdd)
      // Rewrite the AlterTableRenameCommand to AlterHoodieTableRenameCommand
      case AlterTableRenameCommand(oldName, newName, isView)
        if !isView && isHoodieTable(oldName, sparkSession) =>
        new AlterHoodieTableRenameCommand(oldName, newName, isView)
      // Rewrite the AlterTableChangeColumnCommand to AlterHoodieTableChangeColumnCommand
      case AlterTableChangeColumnCommand(tableName, columnName, newColumn)
        if isHoodieTable(tableName, sparkSession) =>
        AlterHoodieTableChangeColumnCommand(tableName, columnName, newColumn)
      // SPARK-34238: the definition of ShowPartitionsCommand has been changed in Spark3.2.
      // Match the class type instead of call the `unapply` method.
      case s: ShowPartitionsCommand
        if isHoodieTable(s.tableName, sparkSession) =>
          ShowHoodieTablePartitionsCommand(s.tableName, s.spec)
      // Rewrite TruncateTableCommand to TruncateHoodieTableCommand
      case TruncateTableCommand(tableName, partitionSpec)
        if isHoodieTable(tableName, sparkSession) =>
        new TruncateHoodieTableCommand(tableName, partitionSpec)
      case _ => plan
    }
  }
}
