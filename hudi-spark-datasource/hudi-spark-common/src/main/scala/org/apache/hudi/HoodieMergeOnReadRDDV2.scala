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
import org.apache.hudi.common.avro.HoodieAvroReaderContext
import org.apache.hudi.common.config.{HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE
import org.apache.hudi.common.expression.{Predicate => HPredicate}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.table.read.{HoodieFileGroupReader, HoodieRecordReader}
import org.apache.hudi.common.table.read.lsm.{HoodieLsmFileGroupReader, LsmReaderUtils}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{HoodieSparkInputMetricsUtils, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{FileFormat, SparkColumnarFileReader}
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter

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
 * @param sqlConf                spark SQL configuration
 * @param fileReaders            suite of base file readers
 * @param tableSchema            table's full schema
 * @param requiredSchema         expected (potentially) projected schema
 * @param targetInstantTime      as-of instant, or last instant in the query timeline
 * @param mergeType              type of merge performed
 * @param fileSplits             target file-splits this RDD will be iterating over
 * @param optionalFilters        filters to apply while reading records
 * @param metaClient             table metadata client
 * @param options                datasource options
 * @param includedInstantTimeSet instant time set used to filter records
 */
class HoodieMergeOnReadRDDV2(@transient sc: SparkContext,
                             @transient config: Configuration,
                             sqlConf: SQLConf,
                             fileReaders: HoodieMergeOnReadBaseFileReaders,
                             tableSchema: HoodieTableSchema,
                             requiredSchema: HoodieTableSchema,
                             targetInstantTime: Option[String],
                             mergeType: String,
                             @transient fileSplits: Seq[HoodieMergeOnReadFileSplit],
                             optionalFilters: Array[Filter],
                             metaClient: HoodieTableMetaClient,
                             options: Map[String, String] = Map.empty,
                             includedInstantTimeSet: Option[Set[String]] = Option.empty)
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD with SparkAdapterSupport {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val hadoopConfBroadcast = sc.broadcast(new SerializableWritable(config))
  private val fileGroupBaseFileReader: Broadcast[SparkColumnarFileReader] = {
    if (!metaClient.isMetadataTable) {
      val updatedOptions: Map[String, String] = options + (FileFormat.OPTION_RETURNING_BATCH -> "false") // disable vectorized reading for MOR
      if (metaClient.getTableConfig.isMultipleBaseFileFormatsEnabled) {
        val parquetReader = sparkAdapter.createParquetFileReader(vectorized = false, sqlConf, updatedOptions, config)
        val orcReader = sparkAdapter.createOrcFileReader(vectorized = false, sqlConf, updatedOptions, config, tableSchema.structTypeSchema)
        val lanceReader = sparkAdapter.createLanceFileReader(vectorized = false, sqlConf, updatedOptions, config).orNull
        val vortexReader = sparkAdapter.createVortexFileReader(vectorized = false, sqlConf, updatedOptions, config).orNull
        val multiReader = new MultipleColumnarFileFormatReader(parquetReader, orcReader, lanceReader, vortexReader)
        sc.broadcast(multiReader)
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.PARQUET) {
        sc.broadcast(sparkAdapter.createParquetFileReader(vectorized = false, sqlConf, updatedOptions, config))
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.ORC) {
        sc.broadcast(sparkAdapter.createOrcFileReader(vectorized = false, sqlConf, updatedOptions, config, tableSchema.structTypeSchema))
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.LANCE) {
        sc.broadcast(sparkAdapter.createLanceFileReader(vectorized = false, sqlConf, updatedOptions, config).orNull)
      } else if (metaClient.getTableConfig.getBaseFileFormat == HoodieFileFormat.VORTEX) {
        sc.broadcast(sparkAdapter.createVortexFileReader(vectorized = false, sqlConf, updatedOptions, config).orNull)
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

  // A split whose required schema has a top-level variant column takes the file-group reader
  // below, whose reader context requests the full-variant projection shape for parquet base
  // files (#19578), so a SHREDDED base file is read on this legacy path through the same
  // contract as everywhere else. Without a top-level variant the base-only split stays on
  // requiredSchemaReaderSkipMerging, whose native VariantType request the Spark 4.1+ parquet
  // reader reconstructs at any depth - the vectorized one at stock settings, since the legacy
  // file format inherits ParquetFileFormat.supportBatch and VariantType is atomic; the variant
  // veto lives in HoodieFileGroupReaderBasedFileFormat only (pinned by TestStreamingSource's
  // nested-only legacy leg), so this is about one contract, not a null read (#19775). Keyed off
  // the adapter building that shape
  // rather than the mere presence of a variant column: it is None below Spark 4.1, where
  // re-routing would cost the fast path for nothing.
  private val shouldRerouteVariantSplit: Boolean =
    sparkAdapter.buildFullVariantReadSchema(requiredSchema.structTypeSchema).isDefined

  // Ordinal each table partition column occupies in the required schema, or -1 when it is not
  // projected. Indexed by the table-config partition-field order, i.e. the very order in which
  // HoodieBaseRelation#getPartitionColumnsAsInternalRow emits the values carried by a split.
  // Resolved on the driver: only this array is shipped to the executors.
  private val partitionColumnOrdinals: Array[Int] = {
    val caseSensitive = sqlConf.caseSensitiveAnalysis
    val requiredFieldNames = requiredSchema.structTypeSchema.fieldNames
    metaClient.getTableConfig.getPartitionFields.orElse(Array.empty[String]).map { partitionColumn =>
      requiredFieldNames.indexWhere(fieldName =>
        if (caseSensitive) fieldName == partitionColumn else fieldName.equalsIgnoreCase(partitionColumn))
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val bytesReadCallback = HoodieSparkInputMetricsUtils.getFSBytesReadOnThreadCallback()

    val iter: Iterator[InternalRow] = partition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty && !shouldRerouteVariantSplit =>
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
          val requestedSchema = requiredSchema.schema
          val instantRange = InstantRange.builder().rangeType(RangeType.EXACT_MATCH).explicitInstants(validInstants.value).build()
          val readerContext = new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig, HOption.of(instantRange), HOption.empty().asInstanceOf[HOption[HPredicate]])
          val fileGroupReader: HoodieFileGroupReader[IndexedRecord] = HoodieFileGroupReader.builder()
            .withReaderContext(readerContext)
            .withHoodieTableMetaClient(metaClient)
            .withLatestCommitTime(targetInstantTime.orNull)
            .withLogFiles(logFiles.stream())
            .withBaseFileOption(baseFileOption)
            .withPartitionPath(partitionPath)
            .withProps(properties)
            .withDataSchema(tableSchema.schema)
            .withRequestedSchema(requestedSchema)
            .withInternalSchemaOpt(HOption.ofNullable(tableSchema.internalSchema.orNull))
            .build()
          convertAvroToRowIterator(fileGroupReader.getClosableIterator, requestedSchema)
        } else {
          val readerContext = new SparkFileFormatInternalRowReaderContext(fileGroupBaseFileReader.value, optionalFilters,
            Seq.empty, storageConf, metaClient.getTableConfig)
          val fileGroupReader: HoodieRecordReader[InternalRow] =
            if (LsmReaderUtils.shouldUseLsmReader(metaClient.getTableConfig, mergeType)) {
              HoodieLsmFileGroupReader.builder[InternalRow]()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(targetInstantTime.orNull)
                .withLogFiles(logFiles.stream())
                .withBaseFileOption(baseFileOption)
                .withPartitionPath(partitionPath)
                .withProps(properties)
                .withDataSchema(tableSchema.schema)
                .withRequestedSchema(requiredSchema.schema)
                .withInternalSchemaOpt(HOption.ofNullable(tableSchema.internalSchema.orNull))
                .build()
            } else {
              HoodieFileGroupReader.builder[InternalRow]()
                .withReaderContext(readerContext)
                .withHoodieTableMetaClient(metaClient)
                .withLatestCommitTime(targetInstantTime.orNull)
                .withLogFiles(logFiles.stream())
                .withBaseFileOption(baseFileOption)
                .withPartitionPath(partitionPath)
                .withProps(properties)
                .withDataSchema(tableSchema.schema)
                .withRequestedSchema(requiredSchema.schema)
                .withInternalSchemaOpt(HOption.ofNullable(tableSchema.internalSchema.orNull))
                .build()
            }
          convertCloseableIterator(fileGroupReader.getClosableIterator, partition.split.partitionValues)
        }
    }

    val commitTimeMetadataFieldIdx = requiredSchema.structTypeSchema.fieldNames.indexOf(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val needsFiltering = commitTimeMetadataFieldIdx >= 0 && includedInstantTimeSet.isDefined
    val resultIter = if (needsFiltering) {
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

    withInputMetrics(resultIter, iter, context, bytesReadCallback)
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
                                       requestedSchema: HoodieSchema): Iterator[InternalRow] = {
    val converter = sparkAdapter.createAvroDeserializer(requestedSchema, requiredSchema.structTypeSchema)
    val projection = UnsafeProjection.create(requiredSchema.structTypeSchema)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = projection.apply(converter.deserialize(closeableFileGroupRecordIterator.next()).get.asInstanceOf[InternalRow])

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  private def convertCloseableIterator(closeableFileGroupRecordIterator: ClosableIterator[InternalRow],
                                       partitionValues: InternalRow): Iterator[InternalRow] = {
    // NOTE: built here, i.e. once per split on the executor -- a projection is not serializable.
    val mapper: InternalRow => InternalRow = partitionValueMapper(partitionValues).getOrElse(identity)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mapper(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  /**
   * Builds the mapper splicing a split's partition values into the rows the file-group reader
   * produces, or None when there is nothing to splice.
   *
   * The file-group reader sources every projected column from the data files, so it hands back
   * nulls for the partition columns whenever those are not persisted there
   * ([["hoodie.datasource.write.drop.partition.columns"]], read-side extraction from the partition
   * path, or a bootstrap data-queries-only read). Only the values parsed off the partition path
   * carry them, which is what a split holds.
   *
   * Binding at existing ordinals is sufficient because the output row shape is already correct:
   * [[org.apache.hudi.common.table.TableSchemaResolver]] re-appends dropped partition columns to
   * the table schema, so the required schema keeps them and the reader merely fills them with
   * nulls. No column is added, moved or dropped here.
   *
   * @param partitionValues values parsed off the partition path, in table-config partition-field
   *                        order (matching [[partitionColumnOrdinals]]), empty when the partition
   *                        columns are read from the data files as usual
   */
  private def partitionValueMapper(partitionValues: InternalRow): Option[InternalRow => InternalRow] = {
    if (partitionValues.numFields == 0 || partitionColumnOrdinals.forall(_ < 0)) {
      None
    } else {
      checkState(partitionValues.numFields == partitionColumnOrdinals.length,
        s"Expected ${partitionColumnOrdinals.length} partition values but got ${partitionValues.numFields}")
      val requiredFields = requiredSchema.structTypeSchema.fields
      // NOTE: The partition values are bound as references into the right half of a JoinedRow rather
      //       than substituted as literals: literals are inlined into the generated code, so every
      //       distinct partition value would miss Spark's codegen cache.
      val projectedFields: Seq[Expression] = requiredFields.zipWithIndex.map { case (field, ordinal) =>
        val partitionFieldIdx = partitionColumnOrdinals.indexOf(ordinal)
        val boundOrdinal = if (partitionFieldIdx >= 0) requiredFields.length + partitionFieldIdx else ordinal
        BoundReference(boundOrdinal, field.dataType, field.nullable)
      }.toSeq
      val projection = UnsafeProjection.create(projectedFields)
      val joinedRow = new JoinedRow()
      Some((row: InternalRow) => projection.apply(joinedRow(row, partitionValues)))
    }
  }

  private def withInputMetrics(iter: Iterator[InternalRow],
                               closeableIter: Iterator[InternalRow],
                               context: TaskContext,
                               bytesReadCallback: () => Long): Iterator[InternalRow] = {
    val metricIter = new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = iter.hasNext

      override def next(): InternalRow = {
        val row = iter.next()
        HoodieSparkInputMetricsUtils.incRecordsRead(context, 1)
        row
      }

      override def close(): Unit = {
        closeableIter match {
          case closeable: Closeable => closeable.close()
          case _ =>
        }
      }
    }

    context.addTaskCompletionListener[Unit] { _ =>
      HoodieSparkInputMetricsUtils.incBytesRead(context, bytesReadCallback())
      metricIter.close()
    }
    metricIter
  }

  private def getPartitionPath(split: HoodieMergeOnReadFileSplit): StoragePath = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile =>
        sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(baseFile))
      .getOrElse(split.logFiles.head.getPath)
      .getParent
  }
}

object HoodieMergeOnReadRDDV2 {
  val CONFIG_INSTANTIATION_LOCK = new Object()
}
