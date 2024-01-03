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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.MergeOnReadSnapshotRelation.createPartitionedFile
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.cdc.{CDCFileGroupIterator, CDCRelation, HoodieCDCFileGroupSplit}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieFileGroupId}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
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
    val recordKeyColumn = tableState.recordKeyField
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", supportBatchResult)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    val augmentedHadoopConf = FSUtils.buildInlineConf(hadoopConf)
    val baseFileReader = super.buildReaderWithPartitionValues(spark, dataSchema, partitionSchema, requiredSchema,
      filters ++ requiredFilters, options, new Configuration(hadoopConf))
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
    val extraProps = spark.sparkContext.broadcast(sparkAdapter.getExtraProps(supportBatchResult, spark.sessionState.conf, options, augmentedHadoopConf))
    val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedHadoopConf))
    val broadcastedDataSchema =  spark.sparkContext.broadcast(dataAvroSchema)
    val broadcastedRequestedSchema =  spark.sparkContext.broadcast(requestedAvroSchema)
    val props: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options)

    (file: PartitionedFile) => {
      val tablePath = new Path(tableState.tablePath)
      lazy val readerContext = new SparkFileFormatInternalRowReaderContext(extraProps.value, tableState.recordKeyField, filters, shouldUseRecordPosition)
      val filePath = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file)
      val filegroupName = FSUtils.getFileIdFromFilePath(filePath)
      file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>

          fileSliceMapping.getSlice(filegroupName) match {
            case Some(fileSlice) if !isCount =>
              if (requiredSchema.isEmpty && !fileSlice.getLogFiles.findAny().isPresent) {
                val hoodieBaseFile = fileSlice.getBaseFile.get()
                baseFileReader(createPartitionedFile(fileSliceMapping.getPartitionValues, hoodieBaseFile.getHadoopPath, 0, hoodieBaseFile.getFileLen))
              } else {
                buildFileGroupReaderIterator(fileSlice, fileSliceMapping.getPartitionValues, broadcastedDataSchema.value,
                  broadcastedRequestedSchema.value, requiredSchema, partitionSchema, outputSchema, file.start, file.length,
                  broadcastedHadoopConf.value.value, readerContext)
              }

            case _ =>
              buildFileGroupReaderIterator(createFileSlice(tablePath,filePath, filegroupName), file.partitionValues, broadcastedDataSchema.value,
                broadcastedRequestedSchema.value, requiredSchema, partitionSchema, outputSchema, file.start, file.length,
                broadcastedHadoopConf.value.value, readerContext)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplits().toArray
          val fileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
          buildCDCRecordIterator(
            fileGroupSplit, cdcFileReader, broadcastedHadoopConf.value.value, props, requiredSchema)

        case _ =>
          buildFileGroupReaderIterator(createFileSlice(tablePath, filePath, filegroupName), file.partitionValues, broadcastedDataSchema.value,
            broadcastedRequestedSchema.value, requiredSchema, partitionSchema, outputSchema, file.start, file.length,
            broadcastedHadoopConf.value.value, readerContext)
      }
    }
  }

  protected def createFileSlice(tablePath: Path, filePath: Path, fileId: String): FileSlice = {
    val fgID = new HoodieFileGroupId(FSUtils.getRelativePartitionPath(tablePath, filePath.getParent), fileId)
    val commitTime = FSUtils.getCommitTime(filePath.toString)
    val baseFile = new HoodieBaseFile(filePath.toString, fileId, commitTime, null)
    new FileSlice(fgID, commitTime, baseFile, java.util.Collections.emptyList())
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

  protected def buildFileGroupReaderIterator(fileSlice: FileSlice,
                                             partitionValues: InternalRow,
                                             dataAvroSchema: Schema,
                                             requestedAvroSchema: Schema,
                                             requiredSchema: StructType,
                                             partitionSchema: StructType,
                                             outputSchema: StructType,
                                             start: Long,
                                             length: Long,
                                             serializedHadoopConf: Configuration,
                                             readerContext: SparkFileFormatInternalRowReaderContext): Iterator[InternalRow] = {
    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
      .builder().setConf(serializedHadoopConf).setBasePath(tableState.tablePath).build
    val reader = new HoodieFileGroupReader[InternalRow](
      readerContext,
      serializedHadoopConf,
      tableState.tablePath,
      tableState.latestCommitTimestamp.get,
      fileSlice,
      dataAvroSchema,
      requestedAvroSchema,
      metaClient.getTableConfig.getProps,
      metaClient.getTableConfig,
      start,
      length,
      shouldUseRecordPosition)
    reader.initRecordIterators()
    // Append partition values to rows and project to output schema
    appendPartitionAndProject(
      reader.getClosableIterator,
      requiredSchema,
      partitionSchema,
      outputSchema,
      partitionValues)
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
