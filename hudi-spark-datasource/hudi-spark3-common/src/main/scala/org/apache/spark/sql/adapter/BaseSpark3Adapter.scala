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

package org.apache.spark.sql.adapter

import org.apache.hudi.{AvroConversionUtils, DefaultSource, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, Spark3HoodiePartitionCDCFileGroupMapping, Spark3HoodiePartitionFileSliceMapping}
import org.apache.hudi.client.model.{HoodieInternalRow, Spark3HoodieInternalRow}
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.spark.internal.ReflectUtil
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, DataFrameUtil, Dataset, HoodieUnsafeUtils, HoodieUTF8StringFactory, Spark3DataFrameUtil, Spark3HoodieUnsafeUtils, Spark3HoodieUTF8StringFactory, SparkSession, SQLContext}
import org.apache.spark.sql.FileFormatUtilsForFileGroupReader.applyFiltersToPlan
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Predicate}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.execution.{PartitionedFileUtil, QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.HoodieFormatTrait
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark3Adapter extends SparkAdapter with Logging {
  JsonUtils.registerModules()

  private val cache = new ConcurrentHashMap[ZoneId, DateFormatter](1)

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def getDateFormatter(tz: TimeZone): DateFormatter = {
    cache.computeIfAbsent(tz.toZoneId, zoneId => ReflectUtil.getDateFormatter(zoneId))
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  /**
   * Checks whether [[LogicalPlan]] refers to Hudi table, and if it's the case extracts
   * corresponding [[CatalogTable]]
   */
  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    EliminateSubqueryAliases(plan) match {
      // First, we need to weed out unresolved plans
      case plan if !plan.resolved => None
      // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
      //       on top of it
      case PhysicalOperation(_, _, LogicalRelation(_, _, Some(table), _)) if isHoodieTable(table) => Some(table)
      case _ => None
    }
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: Schema,
                              parameters: java.util.Map[String, String]): BaseRelation = {
    val dataSchema = Option(schema).map(AvroConversionUtils.convertAvroSchemaToStructType).orNull
    DefaultSource.createRelation(sqlContext, metaClient, dataSchema, parameters.asScala.toMap)
  }

  override def convertStorageLevelToString(level: StorageLevel): String

  override def translateFilter(predicate: Expression,
                               supportNestedPredicatePushdown: Boolean = false): Option[Filter] = {
    DataSourceStrategy.translateFilter(predicate, supportNestedPredicatePushdown)
  }

  override def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch = {
    new ColumnarBatch(vectors, numRows)
  }

  override def sqlExecutionWithNewExecutionId[T](sparkSession: SparkSession,
                                                 queryExecution: QueryExecution,
                                                 name: Option[String])(body: => T): T = {
      SQLExecution.withNewExecutionId(queryExecution, name)(body)
  }

  def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit

  override def createInternalRow(metaFields: Array[UTF8String],
                                sourceRow: InternalRow,
                                sourceContainsMetaFields: Boolean): HoodieInternalRow = {
    new Spark3HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
  }

  override def createPartitionCDCFileGroupMapping(partitionValues: InternalRow,
                                                        fileSplits: List[HoodieCDCFileSplit]): HoodiePartitionCDCFileGroupMapping = {
    new Spark3HoodiePartitionCDCFileGroupMapping(partitionValues, fileSplits)
  }

  override def createPartitionFileSliceMapping(values: InternalRow,
                                                     slices: Map[String, FileSlice]): HoodiePartitionFileSliceMapping = {
    new Spark3HoodiePartitionFileSliceMapping(values, slices)
  }

  override def newParseException(command: Option[String],
                                 exception: AnalysisException,
                                 start: Origin,
                                 stop: Origin): ParseException = {
    new ParseException(command, exception.getMessage, start, stop)
  }

  override def getUTF8StringFactory: HoodieUTF8StringFactory = Spark3HoodieUTF8StringFactory

  override def splitFiles(sparkSession: SparkSession,
                          partitionDirectory: PartitionDirectory,
                          isSplitable: Boolean,
                          maxSplitSize: Long): Seq[PartitionedFile] = {
    partitionDirectory.files.flatMap(file =>
      PartitionedFileUtil.splitFiles(sparkSession, file, file.getPath, isSplitable, maxSplitSize, partitionDirectory.values)
    )
  }

  override def createColumnFromExpression(expression: Expression): Column = new Column(expression)

  override def getExpressionFromColumn(column: Column): Expression = column.expr

  override def getUnsafeUtils: HoodieUnsafeUtils = Spark3HoodieUnsafeUtils

  override def getDataFrameUtil: DataFrameUtil = Spark3DataFrameUtil

  override def internalCreateDataFrame(spark: SparkSession, rdd: RDD[InternalRow], schema: StructType, isStreaming: Boolean = false): DataFrame = {
    spark.internalCreateDataFrame(rdd, schema, isStreaming)
  }

  def createStreamingDataFrame(sqlContext: SQLContext, relation: HadoopFsRelation, requiredSchema: StructType): DataFrame = {
    val logicalRelation = LogicalRelation(relation, isStreaming = true)
    val resolvedSchema = logicalRelation.resolve(requiredSchema, sqlContext.sparkSession.sessionState.analyzer.resolver)
    Dataset.ofRows(sqlContext.sparkSession, applyFiltersToPlan(logicalRelation, requiredSchema, resolvedSchema,
        relation.fileFormat.asInstanceOf[HoodieFormatTrait].getRequiredFilters))
  }
}
