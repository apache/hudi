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

import org.apache.avro.Schema
import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, Spark4HoodiePartitionCDCFileGroupMapping, Spark4HoodiePartitionFileSliceMapping}
import org.apache.hudi.client.model.{HoodieInternalRow, Spark4HoodieInternalRow}
import org.apache.hudi.{AvroConversionUtils, DefaultSource, Spark4RowSerDe}
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit
import org.apache.hudi.common.util.JsonUtils
import org.apache.hudi.common.util.collection.{FlatLists, Spark4FlatLists}
import org.apache.hudi.spark4.internal.ReflectUtil
import org.apache.hudi.storage.StoragePath
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, HoodieSpark4CatalogUtils, SQLContext, SparkSession}
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Predicate}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark4Adapter extends SparkAdapter with Logging {

  JsonUtils.registerModules()

  private val cache = new ConcurrentHashMap[ZoneId, DateFormatter](1)

  def getCatalogUtils: HoodieSpark4CatalogUtils

  override def createSparkRowSerDe(schema: StructType): SparkRowSerDe = {
    new Spark4RowSerDe(getCatalystExpressionUtils.getEncoder(schema))
  }

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def getSparkParsePartitionUtil: SparkParsePartitionUtil = Spark4ParsePartitionUtil

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

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: Schema,
                              globPaths: Array[StoragePath],
                              parameters: java.util.Map[String, String]): BaseRelation = {
    val dataSchema = Option(schema).map(AvroConversionUtils.convertAvroSchemaToStructType).orNull
    DefaultSource.createRelation(sqlContext, metaClient, dataSchema, globPaths, parameters.asScala.toMap)
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

  override def compareUTF8String(a: UTF8String, b: UTF8String): Int = a.binaryCompare(b)

  override def createComparableList(t: Array[AnyRef]): FlatLists.ComparableList[Comparable[HoodieRecord[_]]] = Spark4FlatLists.ofComparableArray(t)

  override def createInternalRow(commitTime: UTF8String,
                                 commitSeqNumber: UTF8String,
                                 recordKey: UTF8String,
                                 partitionPath: UTF8String,
                                 fileName: UTF8String,
                                 sourceRow: InternalRow,
                                 sourceContainsMetaFields: Boolean): HoodieInternalRow = {
    new Spark4HoodieInternalRow(commitTime, commitSeqNumber, recordKey, partitionPath, fileName, sourceRow, sourceContainsMetaFields)
  }

  override def createInternalRow(metaFields: Array[UTF8String],
                                 sourceRow: InternalRow,
                                 sourceContainsMetaFields: Boolean): HoodieInternalRow = {
    new Spark4HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
  }

  override def createHoodiePartitionCDCFileGroupMapping(partitionValues: InternalRow,
                                                        fileSplits: List[HoodieCDCFileSplit]): HoodiePartitionCDCFileGroupMapping = {
    new Spark4HoodiePartitionCDCFileGroupMapping(partitionValues, fileSplits)
  }

  override def createHoodiePartitionFileSliceMapping(values: InternalRow,
                                                     slices: Map[String, FileSlice]): HoodiePartitionFileSliceMapping = {
    new Spark4HoodiePartitionFileSliceMapping(values, slices)
  }

  override def newParseException(command: Option[String],
                                 exception: AnalysisException,
                                 start: Origin,
                                 stop: Origin): ParseException = {
    new ParseException(command, start, stop, exception.getErrorClass, exception.getMessageParameters.asScala.toMap)
  }
}
