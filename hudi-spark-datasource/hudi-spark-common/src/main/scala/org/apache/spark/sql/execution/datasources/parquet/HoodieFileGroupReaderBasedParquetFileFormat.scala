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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.MergeOnReadSnapshotRelation.createPartitionedFile
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.cdc.{CDCFileGroupIterator, CDCRelation, HoodieCDCFileGroupSplit}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMemoryConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.FileIOUtils
import org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieTableSchema, HoodieTableState, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable
import java.util.Locale

trait HoodieFormatTrait {

  // Used so that the planner only projects once and does not stack overflow
  var isProjected: Boolean = false
  def getRequiredFilters: Seq[Filter]
}

/**
 * This class utilizes {@link HoodieFileGroupReader} and its related classes to support reading
 * from Parquet formatted base files and their log files.
 */
class HoodieFileGroupReaderBasedParquetFileFormat(tableState: HoodieTableState,
                                                  tableSchema: HoodieTableSchema,
                                                  tableName: String,
                                                  mergeType: String,
                                                  mandatoryFields: Seq[String],
                                                  isMOR: Boolean,
                                                  isBootstrap: Boolean,
                                                  isIncremental: Boolean,
                                                  shouldUseRecordPosition: Boolean,
                                                  requiredFilters: Seq[Filter]
                                                 ) extends ParquetFileFormat with SparkAdapterSupport with HoodieFormatTrait {

  def getRequiredFilters: Seq[Filter] = requiredFilters

  private val sanitizedTableName = AvroSchemaUtils.getAvroRecordQualifiedName(tableName)

  /**
   * Support batch needs to remain consistent, even if one side of a bootstrap merge can support
   * while the other side can't
   */
  /*
private var supportBatchCalled = false
private var supportBatchResult = false

override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
  if (!supportBatchCalled || supportBatchResult) {
    supportBatchCalled = true
    supportBatchResult = tableSchema.internalSchema.isEmpty && !isMOR && !isIncremental && !isBootstrap && super.supportBatch(sparkSession, schema)
  }
  supportBatchResult
}
 */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = false

  private val supportBatchResult = false

  private lazy val internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema] = if (tableSchema.internalSchema.isEmpty) {
    org.apache.hudi.common.util.Option.empty()
  } else {
    org.apache.hudi.common.util.Option.of(tableSchema.internalSchema.get)
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    //dataSchema is not always right due to spark bugs
    val partitionColumns = partitionSchema.fieldNames
    val dataSchema = StructType(tableSchema.structTypeSchema.fields.filterNot(f => partitionColumns.contains(f.name)))
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", supportBatchResult)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    val augmentedHadoopConf = FSUtils.buildInlineConf(hadoopConf)
    setSchemaEvolutionConfigs(augmentedHadoopConf, options)
    val baseFileReader = super.buildReaderWithPartitionValues(spark, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(augmentedHadoopConf))
    val cdcFileReader = super.buildReaderWithPartitionValues(
      spark,
      tableSchema.structTypeSchema,
      StructType(Nil),
      tableSchema.structTypeSchema,
      Nil,
      options,
      new Configuration(hadoopConf))

    val requestedAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(requiredSchema, sanitizedTableName)
    val dataAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(dataSchema, sanitizedTableName)
    val parquetFileReader = spark.sparkContext.broadcast(sparkAdapter.createParquetFileReader(supportBatchResult, spark.sessionState.conf, options, augmentedHadoopConf))
    val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedHadoopConf))
    val broadcastedDataSchema =  spark.sparkContext.broadcast(dataAvroSchema)
    val broadcastedRequestedSchema =  spark.sparkContext.broadcast(requestedAvroSchema)
    val fileIndexProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options)

    (file: PartitionedFile) => {
      file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val filegroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(filegroupName) match {
            case Some(fileSlice) if !isCount =>
              if (requiredSchema.isEmpty && !fileSlice.getLogFiles.findAny().isPresent) {
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                baseFileReader(createPartitionedFile(fileSliceMapping.getPartitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen))
              } else {
                val readerContext = new SparkFileFormatInternalRowReaderContext(parquetFileReader.value,
                  tableState.recordKeyField, filters, shouldUseRecordPosition)
                val serializedHadoopConf = broadcastedHadoopConf.value.value
                val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
                  .builder().setConf(serializedHadoopConf).setBasePath(tableState.tablePath).build
                val reader = new HoodieFileGroupReader[InternalRow](
                  readerContext,
                  serializedHadoopConf,
                  tableState.tablePath,
                  tableState.latestCommitTimestamp.get,
                  fileSlice,
                  broadcastedDataSchema.value,
                  broadcastedRequestedSchema.value,
                  internalSchemaOpt,
                  metaClient,
                  metaClient.getTableConfig.getProps,
                  metaClient.getTableConfig,
                  file.start,
                  file.length,
                  shouldUseRecordPosition,
                  options.getOrElse(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.defaultValue() + "").toLong,
                  options.getOrElse(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), FileIOUtils.getDefaultSpillableMapBasePath),
                  DiskMapType.valueOf(options.getOrElse(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue().name()).toUpperCase(Locale.ROOT)),
                  options.getOrElse(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue().toString).toBoolean)
                reader.initRecordIterators()
                // Append partition values to rows and project to output schema
                appendPartitionAndProject(
                  reader.getClosableIterator,
                  requiredSchema,
                  partitionSchema,
                  outputSchema,
                  fileSliceMapping.getPartitionValues)
              }

            case _ => parquetFileReader.value.read(file, requiredSchema, partitionSchema, filters,
              broadcastedHadoopConf.value.value)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplits().toArray
          val fileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
          buildCDCRecordIterator(
            fileGroupSplit, cdcFileReader, broadcastedHadoopConf.value.value, fileIndexProps, requiredSchema)

        case _ => parquetFileReader.value.read(file, requiredSchema, partitionSchema, filters,
          broadcastedHadoopConf.value.value)
      }
    }
  }

  protected def setSchemaEvolutionConfigs(conf: Configuration, options: Map[String, String]): Unit = {
    if (internalSchemaOpt.isPresent) {
      options.get(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA).foreach(s => conf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, s))
      options.get(SparkInternalSchemaConverter.HOODIE_TABLE_PATH).foreach(s => conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, s))
      options.get(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST).foreach(s => conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, s))
    }
  }

  protected def buildCDCRecordIterator(cdcFileGroupSplit: HoodieCDCFileGroupSplit,
                                       cdcFileReader: PartitionedFile => Iterator[InternalRow],
                                       hadoopConf: Configuration,
                                       props: TypedProperties,
                                       requiredSchema: StructType): Iterator[InternalRow] = {
    props.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, tableName)
    val cdcSchema = CDCRelation.FULL_CDC_SPARK_SCHEMA
    val metaClient = HoodieTableMetaClient.builder.setBasePath(tableState.tablePath).setConf(hadoopConf).build()
    new CDCFileGroupIterator(
      cdcFileGroupSplit,
      metaClient,
      hadoopConf,
      cdcFileReader,
      tableSchema,
      cdcSchema,
      requiredSchema,
      props)
  }

  private def appendPartitionAndProject(iter: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      projectSchema(iter, inputSchema, to)
    } else {
      val unsafeProjection = generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(joinedRow(d, partitionValues)))
    }
  }

  private def projectSchema(iter: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(d))
  }

  def makeCloseableFileGroupMappingRecordIterator(closeableFileGroupRecordIterator: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                                                  mappingFunction: Function[InternalRow, InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mappingFunction(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }
}
