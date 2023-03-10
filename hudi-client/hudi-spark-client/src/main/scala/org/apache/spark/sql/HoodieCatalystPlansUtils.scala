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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
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
   * Decomposes [[InsertIntoStatement]] into its arguments allowing to accommodate for API
   * changes in Spark 3.3
   */
  def unapplyInsertIntoStatement(plan: LogicalPlan): Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)]

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
}
