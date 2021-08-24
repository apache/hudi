/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.datasources.SparkParsePartitionUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * An interface to adapter the difference between spark2 and spark3
 * in some spark related class.
 */
trait SparkAdapter extends Serializable {

  /**
   * Create the SparkRowSerDe.
   */
  def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe

  /**
   * Convert a AliasIdentifier to TableIdentifier.
   */
  def toTableIdentify(aliasId: AliasIdentifier): TableIdentifier

  /**
   * Convert a UnresolvedRelation to TableIdentifier.
   */
  def toTableIdentify(relation: UnresolvedRelation): TableIdentifier

  /**
   * Create Join logical plan.
   */
  def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join

  /**
   * Test if the logical plan is a Insert Into LogicalPlan.
   */
  def isInsertInto(plan: LogicalPlan): Boolean

  /**
   * Get the member of the Insert Into LogicalPlan.
   */
  def getInsertIntoChildren(plan: LogicalPlan):
    Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)]

  /**
   * Create a Insert Into LogicalPlan.
   */
  def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
    query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan

  /**
   * Create the hoodie's extended spark sql parser.
   */
  def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = None

  /**
   * Create the SparkParsePartitionUtil.
   */
  def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil

  /**
   * Create Like expression.
   */
  def createLike(left: Expression, right: Expression): Expression

  /**
   * ParserInterface#parseMultipartIdentifier is supported since spark3, for spark2 this should not be called.
   */
  def parseMultipartIdentifier(parser: ParserInterface, sqlText: String): Seq[String]
}
