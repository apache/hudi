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
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieSparkUtils, HoodieTableSchema, HoodieTableState, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

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
                val readerContext: HoodieReaderContext[InternalRow] = new SparkFileFormatInternalRowReaderContext(extraProps.value, recordKeyColumn, filters, shouldUseRecordPosition)
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
                  shouldUseRecordPosition)
                reader.initRecordIterators()
                // Append partition values to rows and project to output schema
                appendPartitionAndProject(
                  reader.getClosableIterator.asInstanceOf[java.util.Iterator[InternalRow]].asScala,
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
            fileGroupSplit, cdcFileReader, broadcastedHadoopConf.value.value, props, requiredSchema)
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

  private def appendPartitionAndProject(iter: Iterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      projectSchema(iter, inputSchema, to)
    } else {
      val unsafeProjection = generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      iter.map(d => unsafeProjection(joinedRow(d, partitionValues)))
    }
  }

  private def projectSchema(iter: Iterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    iter.map(d => unsafeProjection(d))
  }

}
