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
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMemoryConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieSparkUtils, HoodieTableSchema, HoodieTableState, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.FileIOUtils
import org.apache.hudi.common.util.collection.ExternalSpillableMap
import org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.HoodieFileGroupReaderBasedParquetFileFormat.{ROW_INDEX, ROW_INDEX_TEMPORARY_COLUMN_NAME, getAppliedFilters, getAppliedRequiredSchema, getLogFilesFromSlice, getRecordKeyRelatedFilters, makeCloseableFileGroupMappingRecordIterator}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isMetaField
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, Metadata, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable
import java.util.Locale
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter

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

  /**
   * Support batch needs to remain consistent, even if one side of a bootstrap merge can support
   * while the other side can't
   */
  private var supportBatchCalled = false
  private var supportBatchResult = false

  private val sanitizedTableName = AvroSchemaUtils.getAvroRecordQualifiedName(tableName)
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    if (!supportBatchCalled || supportBatchResult) {
      supportBatchCalled = true
      supportBatchResult = !isMOR && !isIncremental && !isBootstrap && super.supportBatch(sparkSession, schema)
    }
    supportBatchResult
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
    val requiredSchemaWithMandatory = generateRequiredSchemaWithMandatory(requiredSchema, dataSchema, partitionSchema)
    val isCount = requiredSchemaWithMandatory.isEmpty
    val requiredSchemaSplits = requiredSchemaWithMandatory.fields.partition(f => HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name))
    val requiredMeta = StructType(requiredSchemaSplits._1)
    val requiredWithoutMeta = StructType(requiredSchemaSplits._2)
    val augmentedHadoopConf = FSUtils.buildInlineConf(hadoopConf)
    val (baseFileReader, preMergeBaseFileReader, readerMaps, cdcFileReader) = buildFileReaders(
      spark, dataSchema, partitionSchema, requiredSchema, filters, options, augmentedHadoopConf,
      requiredSchemaWithMandatory, requiredWithoutMeta, requiredMeta)

    val requestedAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(requiredSchema, sanitizedTableName)
    val dataAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(dataSchema, sanitizedTableName)

    val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedHadoopConf))
    val broadcastedDataSchema =  spark.sparkContext.broadcast(dataAvroSchema)
    val broadcastedRequestedSchema =  spark.sparkContext.broadcast(requestedAvroSchema)
    val fileIndexProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options)

    (file: PartitionedFile) => {
      file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val filePath = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
          val filegroupName = if (FSUtils.isLogFile(filePath)) {
            FSUtils.getFileId(filePath.getName).substring(1)
          } else {
            FSUtils.getFileId(filePath.getName)
          }
          fileSliceMapping.getSlice(filegroupName) match {
            case Some(fileSlice) if !isCount =>
              if (requiredSchema.isEmpty && !fileSlice.getLogFiles.findAny().isPresent) {
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                baseFileReader(createPartitionedFile(fileSliceMapping.getPartitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen))
              } else {
                val readerContext: HoodieReaderContext[InternalRow] = new SparkFileFormatInternalRowReaderContext(
                  readerMaps)
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

            // TODO: Use FileGroupReader here: HUDI-6942.
            case _ => baseFileReader(file)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplits().toArray
          val fileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
          buildCDCRecordIterator(
            fileGroupSplit, cdcFileReader, broadcastedHadoopConf.value.value, fileIndexProps, requiredSchema)
        // TODO: Use FileGroupReader here: HUDI-6942.
        case _ => baseFileReader(file)
      }
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

  private def generateRequiredSchemaWithMandatory(requiredSchema: StructType,
                                                  dataSchema: StructType,
                                                  partitionSchema: StructType): StructType = {
    val metaFields = Seq(
      StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType),
      StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, StringType),
      StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType),
      StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, StringType),
      StructField(HoodieRecord.FILENAME_METADATA_FIELD, StringType))

    // Helper method to get the StructField for nested fields
    @tailrec
    def findNestedField(schema: StructType, fieldParts: Array[String]): Option[StructField] = {
      fieldParts.toList match {
        case head :: Nil => schema.fields.find(_.name == head) // If it's the last part, find and return the field
        case head :: tail => // If there are more parts, find the field and its nested fields
          schema.fields.find(_.name == head) match {
            case Some(StructField(_, nested: StructType, _, _)) => findNestedField(nested, tail.toArray)
            case _ => None // The path is not valid
          }
        case _ => None // Empty path, should not happen if the input is correct
      }
    }

    def findMetaField(name: String): Option[StructField] = {
      metaFields.find(f => f.name == name)
    }

    val added: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    for (field <- mandatoryFields) {
      if (requiredSchema.getFieldIndex(field).isEmpty) {
        // Support for nested fields
        val fieldParts = field.split("\\.")
        val fieldToAdd = findNestedField(dataSchema, fieldParts)
          .orElse(findNestedField(partitionSchema, fieldParts))
          .orElse(findMetaField(field))
          .getOrElse(throw new IllegalArgumentException(s"Field $field does not exist in the table schema"))
        added.append(fieldToAdd)
      }
    }
    val addedFields = StructType(added.toArray)
    StructType(requiredSchema.toArray ++ addedFields.fields)
  }

  protected def buildFileReaders(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType,
                                 requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String],
                                 hadoopConf: Configuration, requiredSchemaWithMandatory: StructType,
                                 requiredWithoutMeta: StructType, requiredMeta: StructType):
  (PartitionedFile => Iterator[InternalRow],
    PartitionedFile => Iterator[InternalRow],
    mutable.Map[Long, PartitionedFile => Iterator[InternalRow]],
    PartitionedFile => Iterator[InternalRow]) = {

    val m = scala.collection.mutable.Map[Long, PartitionedFile => Iterator[InternalRow]]()

    val recordKeyRelatedFilters = getRecordKeyRelatedFilters(filters, tableState.recordKeyField)
    val baseFileReader = super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(hadoopConf))
    m.put(generateKey(dataSchema, requiredSchema), baseFileReader)

    // File reader for reading a Hoodie base file that needs to be merged with log files
    // Add support for reading files using inline file system.
    val appliedRequiredSchema: StructType = getAppliedRequiredSchema(
      requiredSchemaWithMandatory, shouldUseRecordPosition, ROW_INDEX_TEMPORARY_COLUMN_NAME)
    val appliedFilters = getAppliedFilters(
      requiredFilters, recordKeyRelatedFilters, shouldUseRecordPosition)
    val preMergeBaseFileReader = super.buildReaderWithPartitionValues(
      sparkSession,
      dataSchema,
      StructType(Nil),
      appliedRequiredSchema,
      appliedFilters,
      options,
      new Configuration(hadoopConf))
    m.put(generateKey(dataSchema, appliedRequiredSchema), preMergeBaseFileReader)

    val cdcFileReader = super.buildReaderWithPartitionValues(
      sparkSession,
      tableSchema.structTypeSchema,
      StructType(Nil),
      tableSchema.structTypeSchema,
      Nil,
      options,
      new Configuration(hadoopConf))

    //Rules for appending partitions and filtering in the bootstrap readers:
    // 1. if it is mor, we don't want to filter data or append partitions
    // 2. if we need to merge the bootstrap base and skeleton files then we cannot filter
    // 3. if we need to merge the bootstrap base and skeleton files then we should never append partitions to the
    //    skeleton reader
    val needMetaCols = requiredMeta.nonEmpty
    val needDataCols = requiredWithoutMeta.nonEmpty

    //file reader for bootstrap skeleton files
    if (needMetaCols && isBootstrap) {
      val key = generateKey(HoodieSparkUtils.getMetaSchema, requiredMeta)
      if (needDataCols || isMOR) {
        // no filter and no append
        m.put(key, super.buildReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty),
          requiredMeta, Seq.empty, options, new Configuration(hadoopConf)))
      } else {
        // filter
        m.put(key, super.buildReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty),
          requiredMeta, filters ++ requiredFilters, options, new Configuration(hadoopConf)))
      }

      val requestedMeta = StructType(requiredSchema.fields.filter(sf => isMetaField(sf.name)))
      m.put(generateKey(HoodieSparkUtils.getMetaSchema, requestedMeta),
        super.buildReaderWithPartitionValues(sparkSession, HoodieSparkUtils.getMetaSchema, StructType(Seq.empty), requestedMeta,
          Seq.empty, options, new Configuration(hadoopConf)))
    }

    //file reader for bootstrap base files
    if (needDataCols && isBootstrap) {
      val dataSchemaWithoutMeta = StructType(dataSchema.fields.filterNot(sf => isMetaField(sf.name)))
      val key = generateKey(dataSchemaWithoutMeta, requiredWithoutMeta)
      if (isMOR || needMetaCols) {
        m.put(key, super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, StructType(Seq.empty), requiredWithoutMeta,
          Seq.empty, options, new Configuration(hadoopConf)))
        // no filter and no append

      } else {
        // filter
        m.put(key, super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, StructType(Seq.empty), requiredWithoutMeta,
          filters ++ requiredFilters, options, new Configuration(hadoopConf)))
      }

      val requestedWithoutMeta = StructType(requiredSchema.fields.filterNot(sf => isMetaField(sf.name)))
      m.put(generateKey(dataSchemaWithoutMeta, requestedWithoutMeta),
        super.buildReaderWithPartitionValues(sparkSession, dataSchemaWithoutMeta, StructType(Seq.empty), requestedWithoutMeta,
          Seq.empty, options, new Configuration(hadoopConf)))
    }

    (baseFileReader, preMergeBaseFileReader, m, cdcFileReader)
  }

  protected def generateKey(dataSchema: StructType, requestedSchema: StructType): Long = {
    AvroConversionUtils.convertStructTypeToAvroSchema(dataSchema, sanitizedTableName).hashCode() + AvroConversionUtils.convertStructTypeToAvroSchema(requestedSchema, sanitizedTableName).hashCode()
  }
}

object HoodieFileGroupReaderBasedParquetFileFormat {
  // From "ParquetFileFormat.scala": The names of the field for record position.
  private val ROW_INDEX = "row_index"
  private val ROW_INDEX_TEMPORARY_COLUMN_NAME = s"_tmp_metadata_$ROW_INDEX"

  // From "namedExpressions.scala": Used to construct to record position field metadata.
  private val FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY = "__file_source_generated_metadata_col"
  private val FILE_SOURCE_METADATA_COL_ATTR_KEY = "__file_source_metadata_col"
  private val METADATA_COL_ATTR_KEY = "__metadata_col"

  def getRecordKeyRelatedFilters(filters: Seq[Filter], recordKeyColumn: String): Seq[Filter] = {
    filters.filter(f => f.references.exists(c => c.equalsIgnoreCase(recordKeyColumn)))
  }

  def getLogFilesFromSlice(fileSlice: FileSlice): List[HoodieLogFile] = {
    fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
  }

  def getFieldMetadata(name: String, internalName: String): Metadata = {
    new MetadataBuilder()
      .putString(METADATA_COL_ATTR_KEY, name)
      .putBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY, value = true)
      .putString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY, internalName)
      .build()
  }

  def getAppliedRequiredSchema(requiredSchema: StructType,
                               shouldUseRecordPosition: Boolean,
                               recordPositionColumn: String): StructType = {
    if (shouldAddRecordPositionColumn(shouldUseRecordPosition)) {
      val metadata = getFieldMetadata(recordPositionColumn, ROW_INDEX_TEMPORARY_COLUMN_NAME)
      val rowIndexField = StructField(recordPositionColumn, LongType, nullable = false, metadata)
      StructType(requiredSchema.fields :+ rowIndexField)
    } else {
      requiredSchema
    }
  }

  def getAppliedFilters(requiredFilters: Seq[Filter],
                        recordKeyRelatedFilters: Seq[Filter],
                        shouldUseRecordPosition: Boolean): Seq[Filter] = {
    if (shouldAddRecordKeyFilters(shouldUseRecordPosition)) {
      requiredFilters ++ recordKeyRelatedFilters
    } else {
      requiredFilters
    }
  }

  def shouldAddRecordPositionColumn(shouldUseRecordPosition: Boolean): Boolean = {
    HoodieSparkUtils.gteqSpark3_5 && shouldUseRecordPosition
  }

  def shouldAddRecordKeyFilters(shouldUseRecordPosition: Boolean): Boolean = {
    (!shouldUseRecordPosition) || HoodieSparkUtils.gteqSpark3_5
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
