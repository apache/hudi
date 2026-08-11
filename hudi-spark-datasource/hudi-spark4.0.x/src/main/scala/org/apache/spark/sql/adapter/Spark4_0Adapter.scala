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

import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, Spark40HoodiePartitionCDCFileGroupMapping, Spark40HoodiePartitionFileSliceMapping}
import org.apache.hudi.client.model.{HoodieInternalRow, Spark40HoodieInternalRow}
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkEnv
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.catalog.{V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{HoodieParquetReadSupport, ParquetFileFormat, Spark40HoodieParquetReadSupport, Spark40LegacyHoodieParquetFileFormat, Spark40ParquetReader}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.vortex.SparkVortexReaderBase
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.hudi.HoodieMemoryStream
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.parser.{HoodieExtendedParserInterface, HoodieSpark4_0ExtendedSqlParser}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Implementation of [[SparkAdapter]] for Spark 4.0.x branch
 */
class Spark4_0Adapter extends BaseSpark4Adapter {

  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] = {
    super.resolveHoodieTable(plan).orElse {
      EliminateSubqueryAliases(plan) match {
        // First, we need to weed out unresolved plans
        case plan if !plan.resolved => None
        // NOTE: When resolving Hudi table we allow [[Filter]]s and [[Project]]s be applied
        //       on top of it
        case PhysicalOperation(_, _, DataSourceV2Relation(v2: V2TableWithV1Fallback, _, _, _, _)) if isHoodieTable(v2) =>
          Some(v2.v1Table)
        case ResolvedTable(_, _, V1Table(v1Table), _) if isHoodieTable(v1Table) =>
          Some(v1Table)
        case _ => None
      }
    }
  }

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils = HoodieSpark40CatalystExpressionUtils

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils = HoodieSpark40CatalystPlanUtils

  override def getSchemaUtils: HoodieSchemaUtils = HoodieSpark40SchemaUtils

  override def newParseException(command: Option[String],
                                 exception: AnalysisException,
                                 start: Origin,
                                 stop: Origin): ParseException = {
    new ParseException(command, start, stop, exception.getErrorClass, exception.getMessageParameters.asScala.toMap)
  }

  override def createAvroSerializer(rootCatalystType: DataType, rootType: HoodieSchema, nullable: Boolean): HoodieAvroSerializer =
    new HoodieSpark4_0AvroSerializer(rootCatalystType, rootType.toAvroSchema, nullable)

  override def createAvroDeserializer(rootType: HoodieSchema, rootCatalystType: DataType): HoodieAvroDeserializer =
    new HoodieSpark4_0AvroDeserializer(rootType.toAvroSchema, rootCatalystType)

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    new HoodieSpark4_0ExtendedSqlParser(spark, delegate)

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] = {
    Some(new Spark40LegacyHoodieParquetFileFormat(appendPartitionValues))
  }

  override def createInternalRow(metaFields: Array[UTF8String],
                                 sourceRow: InternalRow,
                                 sourceContainsMetaFields: Boolean): HoodieInternalRow = {
    new Spark40HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
  }

  override def createPartitionCDCFileGroupMapping(partitionValues: InternalRow,
                                                  fileSplits: List[HoodieCDCFileSplit]): HoodiePartitionCDCFileGroupMapping = {
    new Spark40HoodiePartitionCDCFileGroupMapping(partitionValues, fileSplits)
  }

  override def createPartitionFileSliceMapping(values: InternalRow,
                                               slices: Map[String, FileSlice]): HoodiePartitionFileSliceMapping = {
    new Spark40HoodiePartitionFileSliceMapping(values, slices)
  }

  /**
   * Get parquet file reader
   *
   * @param vectorized true if vectorized reading is not prohibited due to schema, reading mode, etc
   * @param sqlConf    the [[SQLConf]] used for the read
   * @param options    passed as a param to the file format
   * @param hadoopConf some configs will be set for the hadoopConf
   * @return parquet file reader
   */
  override def createParquetFileReader(vectorized: Boolean,
                                       sqlConf: SQLConf,
                                       options: Map[String, String],
                                       hadoopConf: Configuration): SparkColumnarFileReader = {
    Spark40ParquetReader.build(vectorized, sqlConf, options, hadoopConf)
  }

  override def createParquetReadSupport(convertTz: Option[java.time.ZoneId],
                                        enableVectorizedReader: Boolean,
                                        enableTimestampFieldRepair: Boolean,
                                        datetimeRebaseSpec: RebaseSpec,
                                        tableSchemaOpt: HOption[MessageType])
      : HoodieParquetReadSupport = {
    new Spark40HoodieParquetReadSupport(
      convertTz, enableVectorizedReader, enableTimestampFieldRepair,
      datetimeRebaseSpec, getRebaseSpec("LEGACY"), tableSchemaOpt)
  }

  override def createVortexFileReader(vectorized: Boolean,
                                      sqlConf: SQLConf,
                                      options: Map[String, String],
                                      hadoopConf: Configuration): Option[SparkColumnarFileReader] = {
    Some(new SparkVortexReaderBase(vectorized))
  }

  override def getDateTimeRebaseMode(): LegacyBehaviorPolicy.Value = {
    // See Spark3_5Adapter.getDateTimeRebaseMode for the rationale.
    val fromSqlConf = Option(SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE, null))
    val fromSparkConf = Option(SparkEnv.get)
      .flatMap(env => Option(env.conf.get(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key, null)))
    LegacyBehaviorPolicy.withName(
      fromSqlConf.orElse(fromSparkConf)
        .getOrElse(SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE)))
  }

  override def createMemoryStream[T: Encoder](id: Int, sparkSession: SparkSession): HoodieMemoryStream[T] = {
    val memoryStream = new MemoryStream[T](id, sparkSession.sqlContext)
    new HoodieMemoryStream[T] {
      override def addData(data: TraversableOnce[T]): Unit = memoryStream.addData(data)

      override def toDS(): Dataset[T] = memoryStream.toDS()
    }
  }
}
