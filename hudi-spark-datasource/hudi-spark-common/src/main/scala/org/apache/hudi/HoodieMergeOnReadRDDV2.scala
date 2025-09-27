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

package org.apache.hudi

import org.apache.hudi.HoodieBaseRelation.{projectReader, BaseFileReader}
import org.apache.hudi.HoodieMergeOnReadRDDV2.CONFIG_INSTANTIATION_LOCK
import org.apache.hudi.LogFileIterator.getPartitionPath
import org.apache.hudi.avro.HoodieAvroReaderContext
import org.apache.hudi.common.config.{HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.expression.{Predicate => HPredicate}
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.{FileFormat, SparkColumnarFileReader}
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util.function.Predicate

import scala.collection.JavaConverters._

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

/**
 * Class holding base-file readers for 3 different use-cases:
 *
 * <ol>
 *   <li>Full-schema reader: is used when whole row has to be read to perform merging correctly.
 *   This could occur, when no optimizations could be applied and we have to fallback to read the whole row from
 *   the base file and the corresponding delta-log file to merge them correctly</li>
 *
 *   <li>Required-schema reader: is used when it's fine to only read row's projected columns.
 *   This could occur, when row could be merged with corresponding delta-log record while leveraging only
 *   projected columns</li>
 *
 *   <li>Required-schema reader (skip-merging): is used when when no merging will be performed (skip-merged).
 *   This could occur, when file-group has no delta-log files</li>
 * </ol>
 */
private[hudi] case class HoodieMergeOnReadBaseFileReaders(fullSchemaReader: BaseFileReader,
                                                          requiredSchemaReader: BaseFileReader,
                                                          requiredSchemaReaderSkipMerging: BaseFileReader)

/**
 * RDD enabling Hudi's Merge-on-Read (MOR) semantic
 *
 * @param sc                     spark's context
 * @param config                 hadoop configuration
 * @param fileReaders            suite of base file readers
 * @param tableSchema            table's full schema
 * @param requiredSchema         expected (potentially) projected schema
 * @param tableState             table's state
 * @param mergeType              type of merge performed
 * @param fileSplits             target file-splits this RDD will be iterating over
 * @param includedInstantTimeSet instant time set used to filter records
 */
class HoodieMergeOnReadRDDV2(@transient sc: SparkContext,
                             @transient config: Configuration,
                             sqlConf: SQLConf,
                             fileReaders: HoodieMergeOnReadBaseFileReaders,
                             tableSchema: HoodieTableSchema,
                             requiredSchema: HoodieTableSchema,
                             tableState: HoodieTableState,
                             mergeType: String,
                             @transient fileSplits: Seq[HoodieMergeOnReadFileSplit],
                             optionalFilters: Array[Filter],
                             metaClient: HoodieTableMetaClient,
                             options: Map[String, String] = Map.empty,
                             includedInstantTimeSet: Option[Set[String]] = Option.empty,
                             requestedToCompletionTimeMap: Option[Map[String, String]] = Option.empty)
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD with SparkAdapterSupport {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val hadoopConfBroadcast = sc.broadcast(new SerializableWritable(config))
  private val fileGroupBaseFileReader: Broadcast[SparkColumnarFileReader] = {
    if (!metaClient.isMetadataTable) {
      val updatedOptions: Map[String, String] = options + (FileFormat.OPTION_RETURNING_BATCH -> "false") // disable vectorized reading for MOR
      if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled) {
        sc.broadcast(new MultipleColumnarFileFormatReader(
          sparkAdapter.createParquetFileReader(vectorized = false, sqlConf, updatedOptions, config),
          sparkAdapter.createOrcFileReader(vectorized = false, sqlConf, updatedOptions, config, tableSchema.structTypeSchema)
        ))
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.PARQUET) {
        sc.broadcast(sparkAdapter.createParquetFileReader(vectorized = false, sqlConf, updatedOptions, config))
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.ORC) {
        sc.broadcast(sparkAdapter.createOrcFileReader(vectorized = false, sqlConf, updatedOptions, config, tableSchema.structTypeSchema))
      } else {
        throw new IllegalArgumentException(s"Unsupported base file format: ${metaClient.getTableConfig.getBaseFileFormat}")
      }
    } else {
      null
    }
  }

  private val validInstants: Broadcast[java.util.Set[String]] = {
    if (metaClient.isMetadataTable) {
      val dataTableBasePath = getDataTableBasePathFromMetadataTable(metaClient.getBasePath.toString)
      val dataMetaClient = HoodieTableMetaClient.builder().setBasePath(dataTableBasePath).setConf(metaClient.getStorageConf).build()
      val validInstantTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(dataMetaClient, metaClient)
      sc.broadcast(validInstantTimestamps)
    } else {
      null
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[HoodieMergeOnReadPartition]

    val iter: Iterator[InternalRow] = partition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        val projectedReader = projectReader(fileReaders.requiredSchemaReaderSkipMerging, requiredSchema.structTypeSchema)
        projectedReader(dataFileOnlySplit.dataFile.get)

      case _ =>
        val hadoopConf = getHadoopConf
        val properties = TypedProperties.fromMap(options.asJava)
        properties.setProperty(MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxCompactionMemoryInBytes))
        properties.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), mergeType)
        val storageConf = new HadoopStorageConfiguration(hadoopConf)

        val baseFileOption = HOption.ofNullable(
          partition.split.dataFile
            .map(file => new HoodieBaseFile(sparkAdapter.getSparkPartitionedFileUtils.getStringPathFromPartitionedFile(file)))
            .orNull)
        val logFiles = partition.split.logFiles.asJava
        val fullPartitionPath = getPartitionPath(partition.split)
        val partitionPath = FSUtils.getRelativePartitionPath(metaClient.getBasePath, fullPartitionPath)

        if (metaClient.isMetadataTable) {
          val requestedSchema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
          val instantRange = InstantRange.builder().rangeType(RangeType.EXACT_MATCH).explicitInstants(validInstants.value).build()
          val readerContext = new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig, HOption.of(instantRange), HOption.empty().asInstanceOf[HOption[HPredicate]])
          val fileGroupReader: HoodieFileGroupReader[IndexedRecord] = HoodieFileGroupReader.newBuilder()
            .withReaderContext(readerContext)
            .withHoodieTableMetaClient(metaClient)
            .withLatestCommitTime(tableState.latestCommitTimestamp.orNull)
            .withLogFiles(logFiles.stream())
            .withBaseFileOption(baseFileOption)
            .withPartitionPath(partitionPath)
            .withProps(properties)
            .withDataSchema(new Schema.Parser().parse(tableSchema.avroSchemaStr))
            .withRequestedSchema(requestedSchema)
            .withInternalSchema(HOption.ofNullable(tableSchema.internalSchema.orNull))
            .build()
          convertAvroToRowIterator(fileGroupReader.getClosableIterator, requestedSchema)
        } else {
          val readerContext = new SparkFileFormatInternalRowReaderContext(fileGroupBaseFileReader.value, optionalFilters,
            Seq.empty, storageConf, metaClient.getTableConfig)
          val fileGroupReader = HoodieFileGroupReader.newBuilder()
            .withReaderContext(readerContext)
            .withHoodieTableMetaClient(metaClient)
            .withLatestCommitTime(tableState.latestCommitTimestamp.orNull)
            .withLogFiles(logFiles.stream())
            .withBaseFileOption(baseFileOption)
            .withPartitionPath(partitionPath)
            .withProps(properties)
            .withDataSchema(new Schema.Parser().parse(tableSchema.avroSchemaStr))
            .withRequestedSchema(new Schema.Parser().parse(requiredSchema.avroSchemaStr))
            .withInternalSchema(HOption.ofNullable(tableSchema.internalSchema.orNull))
            .build()
          convertCloseableIterator(fileGroupReader.getClosableIterator)
        }
    }

    if (iter.isInstanceOf[Closeable]) {
      // register a callback to close logScanner which will be executed on task completion.
      // when tasks finished, this method will be called, and release resources.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.asInstanceOf[Closeable].close()))
    }

    val commitTimeMetadataFieldIdx = requiredSchema.structTypeSchema.fieldNames.indexOf(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val needsFiltering = commitTimeMetadataFieldIdx >= 0 && includedInstantTimeSet.isDefined
    val filteredIter = if (needsFiltering) {
      val filterT: Predicate[InternalRow] = new Predicate[InternalRow] {
        override def test(row: InternalRow): Boolean = {
          val commitTime = row.getString(commitTimeMetadataFieldIdx)
          includedInstantTimeSet.get.contains(commitTime)
        }
      }
      iter.filter(filterT.test)
    } else {
      iter
    }

    if (requestedToCompletionTimeMap.isDefined && metaClient.getTableConfig.getTableVersion.versionCode() > 6 && commitTimeMetadataFieldIdx >= 0) {
      val timeMap = requestedToCompletionTimeMap.get
      filteredIter.map { row =>
        val currentRequestedTime = row.getString(commitTimeMetadataFieldIdx)
        val completionTime = timeMap.getOrElse(currentRequestedTime, currentRequestedTime)
        val originalValues = (0 until row.numFields).map { i =>
          row.get(i, requiredSchema.structTypeSchema.fields(i).dataType)
        }
        val newValues = originalValues :+ UTF8String.fromString(completionTime)
        InternalRow.fromSeq(newValues)
      }
    } else {
      filteredIter
    }
  }

  override protected def getPartitions: Array[Partition] =
    fileSplits.zipWithIndex.map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray

  private def getHadoopConf: Configuration = {
    val conf = hadoopConfBroadcast.value.value
    // TODO clean up, this lock is unnecessary
    CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }

  private def convertAvroToRowIterator(closeableFileGroupRecordIterator: ClosableIterator[IndexedRecord],
                                       requestedSchema: Schema): Iterator[InternalRow] = {
    val converter = sparkAdapter.createAvroDeserializer(requestedSchema, requiredSchema.structTypeSchema)
    val projection = UnsafeProjection.create(requiredSchema.structTypeSchema)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = projection.apply(converter.deserialize(closeableFileGroupRecordIterator.next()).get.asInstanceOf[InternalRow])

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  private def convertCloseableIterator(closeableFileGroupRecordIterator: ClosableIterator[InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = closeableFileGroupRecordIterator.next()

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }
}

object HoodieMergeOnReadRDDV2 {
  val CONFIG_INSTANTIATION_LOCK = new Object()
}

