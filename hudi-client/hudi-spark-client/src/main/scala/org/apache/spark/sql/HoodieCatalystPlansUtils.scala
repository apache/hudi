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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

trait HoodieCatalystPlansUtils {

  /**
   * Resolves output of the provided [[query]] against the [[expected]] list of [[Attribute]],
   * and returns new (reshaped) instance of the [[LogicalPlan]]
   *
   * @param tableName used purely for more human-readable error output (if any)
   * @param expected list of attributes output of the query has to adhere to
   * @param query query whose output has to be reshaped
   * @param byName whether the matching should occur by-name or positionally
   * @param conf instance of [[SQLConf]]
   * @return [[LogicalPlan]] which output is aligned to match to that of [[expected]]
   */
  def resolveOutputColumns(tableName: String,
                           expected: Seq[Attribute],
                           query: LogicalPlan,
                           byName: Boolean,
                           conf: SQLConf): LogicalPlan

  /**
   * Instantiates an [[Explain]] command
   */
  def createExplainCommand(plan: LogicalPlan, extended: Boolean): LogicalPlan

  /**
   * Create Join logical plan.
   */
  def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join

  /**
   * Decomposes [[MatchMergeIntoTable]] into its arguments with accommodation for
   * case class changes of [[MergeIntoTable]] in Spark 3.4.
   *
   * Before Spark 3.4.0 (five arguments):
   *
   * case class MergeIntoTable(
   * targetTable: LogicalPlan,
   * sourceTable: LogicalPlan,
   * mergeCondition: Expression,
   * matchedActions: Seq[MergeAction],
   * notMatchedActions: Seq[MergeAction]) extends BinaryCommand with SupportsSubquery
   *
   * Since Spark 3.4.0 (six arguments):
   *
   * case class MergeIntoTable(
   * targetTable: LogicalPlan,
   * sourceTable: LogicalPlan,
   * mergeCondition: Expression,
   * matchedActions: Seq[MergeAction],
   * notMatchedActions: Seq[MergeAction],
   * notMatchedBySourceActions: Seq[MergeAction]) extends BinaryCommand with SupportsSubquery
   */
  def unapplyMergeIntoTable(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)]

  /**
   * Decomposes [[MatchCreateIndex]] into its arguments with accommodation.
   */
  def unapplyCreateIndex(plan: LogicalPlan): Option[(LogicalPlan, String, String, Boolean, Seq[(Seq[String], Map[String, String])], Map[String, String])]

  /**
   * Decomposes [[MatchDropIndex]] into its arguments with accommodation.
   */
  def unapplyDropIndex(plan: LogicalPlan): Option[(LogicalPlan, String, Boolean)]

  /**
   * Decomposes [[MatchShowIndexes]] into its arguments with accommodation.
   */
  def unapplyShowIndexes(plan: LogicalPlan): Option[(LogicalPlan, Seq[Attribute])]

  /**
   * Decomposes [[MatchRefreshIndex]] into its arguments with accommodation.
   */
  def unapplyRefreshIndex(plan: LogicalPlan): Option[(LogicalPlan, String)]

  /**
   * Spark requires file formats to append the partition path fields to the end of the schema.
   * For tables where the partition path fields are not at the end of the schema, we don't want
   * to return the schema in the wrong order when they do a query like "select *". To fix this
   * behavior, we apply a projection onto FileScan when the file format has HoodieFormatTrait
   *
   * Additionally, incremental queries require filters to be added to the plan
   */
  def maybeApplyForNewFileFormat(plan: LogicalPlan): LogicalPlan

  /**
   * Decomposes [[InsertIntoStatement]] into its arguments allowing to accommodate for API
   * changes in Spark 3.3
   * @return a option tuple with (table logical plan, userSpecifiedCols, partitionSpec, query, overwrite, ifPartitionNotExists)
   *         userSpecifiedCols: only than the version of Spark32 will return, other is empty
   */
  def unapplyInsertIntoStatement(plan: LogicalPlan): Option[(LogicalPlan, Seq[String], Map[String, Option[String]], LogicalPlan, Boolean, Boolean)]

  /**
   * Decomposes [[CreateTableLikeCommand]] into its arguments allowing to accommodate for API
   * changes in Spark 3
   */
  def unapplyCreateTableLikeCommand(plan: LogicalPlan): Option[(TableIdentifier, TableIdentifier, CatalogStorageFormat, Option[String], Map[String, String], Boolean)]

  /**
   * Rebases instance of {@code InsertIntoStatement} onto provided instance of {@code targetTable} and {@code query}
   */
  def rebaseInsertIntoStatement(iis: LogicalPlan, targetTable: LogicalPlan, query: LogicalPlan): LogicalPlan

  /**
   * Test if the logical plan is a Repair Table LogicalPlan.
   */
  def isRepairTable(plan: LogicalPlan): Boolean

  /**
   * Get the member of the Repair Table LogicalPlan.
   */
  def getRepairTableChildren(plan: LogicalPlan): Option[(TableIdentifier, Boolean, Boolean, String)]

  /**
   * Calls fail analysis on
   *s
   */
  def failAnalysisForMIT(a: Attribute, cols: String): Unit = {}

  /**
   * Throws TABLE_OR_VIEW_NOT_FOUND error for non-existent table
   */
  def failTableNotFound(tableName: String): Unit = {}

  def createMITJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression], hint: String): LogicalPlan

  /**
   * true if both plans produce the same attributes in the same order
   */
  def produceSameOutput(a: LogicalPlan, b: LogicalPlan): Boolean

  /**
   * Add a project to use the table column names for INSERT INTO BY NAME with specified cols
   */
  def createProjectForByNameQuery(lr: LogicalRelation, plan: LogicalPlan): Option[LogicalPlan]

  /**
   * Decomposes [[UpdateAction]] into its arguments with accommodation for
   * case class changes in Spark 4.1 which added a third parameter `fromStar`.
   *
   * Before Spark 4.1 (two arguments):
   *   case class UpdateAction(condition: Option[Expression], assignments: Seq[Assignment])
   *
   * Since Spark 4.1 (three arguments):
   *   case class UpdateAction(condition: Option[Expression], assignments: Seq[Assignment], fromStar: Boolean)
   */
  def unapplyUpdateAction(mergeAction: Any): Option[(Option[Expression], Seq[Assignment])]
}
