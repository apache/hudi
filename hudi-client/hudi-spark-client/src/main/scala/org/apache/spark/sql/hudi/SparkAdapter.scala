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

import org.apache.avro.Schema
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSchemaConverters, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FilePartition, LogicalRelation, PartitionedFile, SparkParsePartitionUtil}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, Row, SparkSession}

import java.util.Locale

/**
 * Interface adapting discrepancies and incompatibilities between different Spark versions
 */
trait SparkAdapter extends Serializable {

  /**
   * Creates instance of [[HoodieCatalystExpressionUtils]] providing for common utils operating
   * on Catalyst Expressions
   */
  def createCatalystExpressionUtils(): HoodieCatalystExpressionUtils

  /**
   * Creates instance of [[HoodieAvroSerializer]] providing for ability to serialize
   * Spark's [[InternalRow]] into Avro payloads
   */
  def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer

  /**
   * Creates instance of [[HoodieAvroDeserializer]] providing for ability to deserialize
   * Avro payloads into Spark's [[InternalRow]]
   */
  def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer

  /**
   * Creates instance of [[HoodieAvroSchemaConverters]] allowing to convert b/w Avro and Catalyst schemas
   */
  def getAvroSchemaConverters: HoodieAvroSchemaConverters

  /**
   * Create the SparkRowSerDe.
   */
  def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe

  /**
   * Convert a AliasIdentifier to TableIdentifier.
   */
  def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier

  /**
   * Convert a UnresolvedRelation to TableIdentifier.
   */
  def toTableIdentifier(relation: UnresolvedRelation): TableIdentifier

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
   * if the logical plan is a TimeTravelRelation LogicalPlan.
   */
  def isRelationTimeTravel(plan: LogicalPlan): Boolean

  /**
   * Get the member of the TimeTravelRelation LogicalPlan.
   */
  def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])]

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

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  def getFilePartitions(sparkSession: SparkSession, partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition]

  def isHoodieTable(table: LogicalPlan, spark: SparkSession): Boolean = {
    unfoldSubqueryAliases(table) match {
      case LogicalRelation(_, _, Some(table), _) => isHoodieTable(table)
      case relation: UnresolvedRelation =>
        isHoodieTable(toTableIdentifier(relation), spark)
      case _=> false
    }
  }

  def isHoodieTable(map: java.util.Map[String, String]): Boolean = {
    map.getOrDefault("provider", "").equals("hudi")
  }

  def isHoodieTable(table: CatalogTable): Boolean = {
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHoodieTable(table)
  }

  protected def unfoldSubqueryAliases(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case SubqueryAlias(_, relation: LogicalPlan) =>
        unfoldSubqueryAliases(relation)
      case other =>
        other
    }
  }

  /**
    * Create custom Resolution Rule to deal with alter command for hudi.
    */
  def createResolveHudiAlterTableCommand(): Option[SparkSession => Rule[LogicalPlan]] = None

  /**
    * Create instance of [[ParquetFileFormat]]
    */
  def createHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat]
}
