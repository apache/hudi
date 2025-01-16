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

import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieTableSchema, HoodieTableState, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.cdc.{CDCFileGroupIterator, CDCRelation, HoodieCDCFileGroupSplit}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.HoodieFileGroupReaderBasedFileFormat.PARQUET_FILE_EXTENSION
import org.apache.spark.sql.execution.datasources.{HoodieFileGroupReaderBasedFileFormat, PartitionedFile}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils}
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

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
                                                  isCDC: Boolean,
                                                  validCommits: String,
                                                  shouldUseRecordPosition: Boolean,
                                                  requiredFilters: Seq[Filter])
  extends HoodieFileGroupReaderBasedFileFormat(
    tableState,
    tableSchema,
    tableName,
    mandatoryFields,
    isMOR,
    isBootstrap,
    isIncremental,
    validCommits,
    shouldUseRecordPosition,
    requiredFilters) with SparkAdapterSupport  {
  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    val augmentedStorageConf = new HadoopStorageConfiguration(hadoopConf).getInline
    setSchemaEvolutionConfigs(augmentedStorageConf)
    val (remainingPartitionSchemaArr, fixedPartitionIndexesArr) = partitionSchema.fields.toSeq.zipWithIndex.filter(p => !mandatoryFields.contains(p._1.name)).unzip

    // The schema of the partition cols we want to append the value instead of reading from the file
    val remainingPartitionSchema = StructType(remainingPartitionSchemaArr)

    // index positions of the remainingPartitionSchema fields in partitionSchema
    val fixedPartitionIndexes = fixedPartitionIndexesArr.toSet

    // schema that we want fg reader to output to us
    val requestedSchema = StructType(requiredSchema.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name)))
    val requestedAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(requestedSchema, sanitizedTableName)
    val dataAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(dataSchema, sanitizedTableName)
    val parquetFileReader = spark.sparkContext.broadcast(sparkAdapter.createParquetFileReader(
      supportBatchResult, spark.sessionState.conf, options, augmentedStorageConf.unwrap()))
    val broadcastedStorageConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedStorageConf.unwrap()))
    val fileIndexProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options, null)

    (file: PartitionedFile) => {
      val storageConf = new HadoopStorageConfiguration(broadcastedStorageConf.value.value)
      file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val filegroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(filegroupName) match {
            case Some(fileSlice) if !isCount && (requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
              val fileReaders = new java.util.HashMap[String, SparkFileReader]()
              fileReaders.put(PARQUET_FILE_EXTENSION, parquetFileReader.value)
              val readerContext = new SparkFileFormatInternalRowReaderContext(fileReaders, filters, requiredFilters)
              val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
                .builder().setConf(storageConf).setBasePath(tableState.tablePath).build
              val props = metaClient.getTableConfig.getProps
              options.foreach(kv => props.setProperty(kv._1, kv._2))
              val reader = new HoodieFileGroupReader[InternalRow](
                readerContext,
                new HoodieHadoopStorage(metaClient.getBasePath, storageConf),
                tableState.tablePath,
                tableState.latestCommitTimestamp.get,
                fileSlice,
                dataAvroSchema,
                requestedAvroSchema,
                internalSchemaOpt,
                metaClient,
                props,
                file.start,
                file.length,
                shouldUseRecordPosition)
              reader.initRecordIterators()
              // Append partition values to rows and project to output schema
              appendPartitionAndProject(
                reader.getClosableIterator,
                requestedSchema,
                remainingPartitionSchema,
                outputSchema,
                fileSliceMapping.getPartitionValues,
                fixedPartitionIndexes)

            case _ =>
              readBaseFile(file, parquetFileReader.value, requestedSchema, remainingPartitionSchema, fixedPartitionIndexes,
                requiredSchema, partitionSchema, outputSchema, filters, storageConf)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping, parquetFileReader.value, storageConf, fileIndexProps, requiredSchema)

        case _ =>
          readBaseFile(file, parquetFileReader.value, requestedSchema, remainingPartitionSchema, fixedPartitionIndexes,
            requiredSchema, partitionSchema, outputSchema, filters, storageConf)
      }
    }
  }
}
