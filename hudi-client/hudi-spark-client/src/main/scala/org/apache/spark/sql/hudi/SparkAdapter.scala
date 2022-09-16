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
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSchemaConverters, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, LogicalRelation, PartitionedFile, SparkParsePartitionUtil}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, HoodieCatalystPlansUtils, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Locale
import java.util.{Map => JMap}

/**
 * Interface adapting discrepancies and incompatibilities between different Spark versions
 */
trait SparkAdapter extends Serializable {

  /**
   * Creates instance of [[HoodieCatalystExpressionUtils]] providing for common utils operating
   * on Catalyst [[Expression]]s
   */
  def getCatalystExpressionUtils: HoodieCatalystExpressionUtils

  /**
   * Creates instance of [[HoodieCatalystPlansUtils]] providing for common utils operating
   * on Catalyst [[LogicalPlan]]s
   */
  def getCatalystPlanUtils: HoodieCatalystPlansUtils

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
   * Create the hoodie's extended spark sql parser.
   */
  def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = None

  /**
   * Create the SparkParsePartitionUtil.
   */
  def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil

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
        isHoodieTable(getCatalystPlanUtils.toTableIdentifier(relation), spark)
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
   * Create instance of [[ParquetFileFormat]]
   */
  def createHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat]

  /**
   * Create instance of [[InterpretedPredicate]]
   *
   * TODO move to HoodieCatalystExpressionUtils
   */
  def createInterpretedPredicate(e: Expression): InterpretedPredicate

  /**
   * Create Hoodie relation based on globPaths, otherwise use tablePath if it's empty
   */
  def createRelation(metaClient: HoodieTableMetaClient,
                     sqlContext: SQLContext,
                     schema: Schema,
                     globPaths: Array[Path],
                     parameters: JMap[String, String]): BaseRelation

  /**
   * Create instance of [[HoodieFileScanRDD]]
   * SPARK-37273 FileScanRDD constructor changed in SPARK 3.3
   */
  def createHoodieFileScanRDD(sparkSession: SparkSession,
                              readFunction: PartitionedFile => Iterator[InternalRow],
                              filePartitions: Seq[FilePartition],
                              readDataSchema: StructType,
                              metadataColumns: Seq[AttributeReference] = Seq.empty): FileScanRDD

  /**
   * Resolve [[DeleteFromTable]]
   * SPARK-38626 condition is no longer Option in Spark 3.3
   */
  def resolveDeleteFromTable(deleteFromTable: Command,
                             resolveExpression: Expression => Expression): LogicalPlan

  /**
   * Extract condition in [[DeleteFromTable]]
   * SPARK-38626 condition is no longer Option in Spark 3.3
   */
  def extractDeleteCondition(deleteFromTable: Command): Expression

  /**
   * Get parseQuery from ExtendedSqlParser, only for Spark 3.3+
   */
  def getQueryParserFromExtendedSqlParser(session: SparkSession, delegate: ParserInterface,
                                          sqlText: String): LogicalPlan = {
    // unsupported by default
    throw new UnsupportedOperationException(s"Unsupported parseQuery method in Spark earlier than Spark 3.3.0")
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  def convertStorageLevelToString(level: StorageLevel): String
}
