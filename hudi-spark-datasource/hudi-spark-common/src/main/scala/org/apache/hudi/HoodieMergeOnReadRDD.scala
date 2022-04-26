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

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.HoodieMergeOnReadRDD.{AvroDeserializerSupport, collectFieldOrdinals, getPartitionPath, projectAvro, projectAvroUnsafe, projectRowUnsafe, resolveAvroSchemaNullability}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{HoodieLogFile, HoodieRecord, HoodieRecordPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import java.io.Closeable
import java.util.Properties
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

case class MergeOnReadBaseFileReaders(fullSchemaFileReader: PartitionedFile => Iterator[InternalRow],
                                      requiredSchemaFileReaderForMerging: PartitionedFile => Iterator[InternalRow],
                                      requiredSchemaFileReaderForNoMerging: PartitionedFile => Iterator[InternalRow])

class HoodieMergeOnReadRDD(@transient sc: SparkContext,
                           @transient config: Configuration,
                           fileReaders: MergeOnReadBaseFileReaders,
                           dataSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           tableState: HoodieTableState,
                           mergeType: String,
                           @transient fileSplits: Seq[HoodieMergeOnReadFileSplit])
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val confBroadcast = sc.broadcast(new SerializableWritable(config))
  private val payloadProps = tableState.preCombineFieldOpt
    .map(preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    )
    .getOrElse(new Properties())

  private val whitelistedPayloadClasses: Set[String] = Seq(
    classOf[OverwriteWithLatestAvroPayload]
  ).map(_.getName).toSet

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeOnReadPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val iter = mergeOnReadPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        fileReaders.requiredSchemaFileReaderForNoMerging.apply(dataFileOnlySplit.dataFile.get)

      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        new LogFileIterator(logFileOnlySplit, getConfig)

      case split if mergeType.equals(DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL) =>
        val baseFileIterator = fileReaders.requiredSchemaFileReaderForNoMerging.apply(split.dataFile.get)
        new SkipMergeIterator(split, baseFileIterator, getConfig)

      case split if mergeType.equals(DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL) =>
        val (baseFileIterator, schema) = readBaseFile(split)
        new RecordMergingFileIterator(split, baseFileIterator, schema, getConfig)

      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${mergeOnReadPartition.split.dataFile.get.filePath}" +
        s"log paths: ${mergeOnReadPartition.split.logFiles.toString}" +
        s"hoodie table path: ${tableState.tablePath}" +
        s"spark partition Index: ${mergeOnReadPartition.index}" +
        s"merge type: ${mergeType}")
    }

    if (iter.isInstanceOf[Closeable]) {
      // register a callback to close logScanner which will be executed on task completion.
      // when tasks finished, this method will be called, and release resources.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.asInstanceOf[Closeable].close()))
    }

    iter
  }

  private def readBaseFile(split: HoodieMergeOnReadFileSplit): (Iterator[InternalRow], HoodieTableSchema) = {
    // NOTE: This is an optimization making sure that even for MOR tables we fetch absolute minimum
    //       of the stored data possible, while still properly executing corresponding relation's semantic
    //       and meet the query's requirements.
    //
    //       Here we assume that iff queried table
    //          a) It does use one of the standard (and whitelisted) Record Payload classes
    //       then we can avoid reading and parsing the records w/ _full_ schema, and instead only
    //       rely on projected one, nevertheless being able to perform merging correctly
    if (!whitelistedPayloadClasses.contains(tableState.recordPayloadClassName))
      (fileReaders.fullSchemaFileReader(split.dataFile.get), dataSchema)
    else
      (fileReaders.requiredSchemaFileReaderForMerging(split.dataFile.get), requiredSchema)
  }

  override protected def getPartitions: Array[Partition] =
    fileSplits.zipWithIndex.map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray

  private def getConfig: Configuration = {
    val conf = confBroadcast.value.value
    HoodieMergeOnReadRDD.CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }

  /**
   * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], iterates over all of the records stored in
   * Delta Log files (represented as [[InternalRow]]s)
   */
  private class LogFileIterator(split: HoodieMergeOnReadFileSplit,
                                config: Configuration)
    extends Iterator[InternalRow] with Closeable with AvroDeserializerSupport {

    protected override val requiredAvroSchema: Schema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
    protected override val requiredStructTypeSchema: StructType = requiredSchema.structTypeSchema

    protected val logFileReaderAvroSchema: Schema = new Schema.Parser().parse(dataSchema.avroSchemaStr)

    protected val recordBuilder: GenericRecordBuilder = new GenericRecordBuilder(requiredAvroSchema)
    protected var recordToLoad: InternalRow = _

    // TODO validate whether we need to do UnsafeProjection
    protected val unsafeProjection: UnsafeProjection = UnsafeProjection.create(requiredStructTypeSchema)

    // NOTE: This maps _required_ schema fields onto the _full_ table schema, collecting their "ordinals"
    //       w/in the record payload. This is required, to project records read from the Delta Log file
    //       which always reads records in full schema (never projected, due to the fact that DL file might
    //       be stored in non-columnar formats like Avro, HFile, etc)
    private val requiredSchemaFieldOrdinals: List[Int] = collectFieldOrdinals(requiredAvroSchema, logFileReaderAvroSchema)

    private var logScanner = {
      val internalSchema = dataSchema.internalSchema.getOrElse(InternalSchema.getEmptyInternalSchema)
      HoodieMergeOnReadRDD.scanLog(split.logFiles, getPartitionPath(split), logFileReaderAvroSchema, tableState,
        maxCompactionMemoryInBytes, config, internalSchema)
    }

    private val logRecords = logScanner.getRecords.asScala

    // NOTE: This iterator iterates over already projected (in required schema) records
    // NOTE: This have to stay lazy to make sure it's initialized only at the point where it's
    //       going to be used, since we modify `logRecords` before that and therefore can't do it any earlier
    protected lazy val logRecordsIterator: Iterator[Option[GenericRecord]] =
      logRecords.iterator.map {
        case (_, record) =>
          val avroRecordOpt = toScalaOption(record.getData.getInsertValue(logFileReaderAvroSchema, payloadProps))
          avroRecordOpt.map {
            avroRecord => projectAvroUnsafe(avroRecord, requiredAvroSchema, requiredSchemaFieldOrdinals, recordBuilder)
          }
      }

    protected def removeLogRecord(key: String): Option[HoodieRecord[_ <: HoodieRecordPayload[_]]] =
      logRecords.remove(key)

    override def hasNext: Boolean = hasNextInternal

    // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
    //       that recursion is unfolded into a loop to avoid stack overflows while
    //       handling records
    @tailrec private def hasNextInternal: Boolean = {
      logRecordsIterator.hasNext && {
        val avroRecordOpt = logRecordsIterator.next()
        if (avroRecordOpt.isEmpty) {
          // Record has been deleted, skipping
          this.hasNextInternal
        } else {
          recordToLoad = unsafeProjection(deserialize(avroRecordOpt.get))
          true
        }
      }
    }

    override final def next(): InternalRow = recordToLoad

    override def close(): Unit =
      if (logScanner != null) {
        try {
          logScanner.close()
        } finally {
          logScanner = null
        }
      }
  }

  /**
   * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
   * Base file as well as all of the Delta Log files simply returning concatenation of these streams, while not
   * performing any combination/merging of the records w/ the same primary keys (ie producing duplicates potentially)
   */
  private class SkipMergeIterator(split: HoodieMergeOnReadFileSplit,
                                  baseFileIterator: Iterator[InternalRow],
                                  config: Configuration)
    extends LogFileIterator(split, config) {

    override def hasNext: Boolean = {
      if (baseFileIterator.hasNext) {
        val curRow = baseFileIterator.next()
        recordToLoad = unsafeProjection(curRow)
        true
      } else {
        super[LogFileIterator].hasNext
      }
    }
  }

  /**
   * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
   * a) Base file and all of the b) Delta Log files combining records with the same primary key from both of these
   * streams
   */
  private class RecordMergingFileIterator(split: HoodieMergeOnReadFileSplit,
                                          baseFileIterator: Iterator[InternalRow],
                                          baseFileReaderSchema: HoodieTableSchema,
                                          config: Configuration)
    extends LogFileIterator(split, config) {

    // NOTE: Record-merging iterator supports 2 modes of operation merging records bearing either
    //        - Full table's schema
    //        - Projected schema
    //       As such, no particular schema could be assumed, and therefore we rely on the caller
    //       to correspondingly set the scheme of the expected output of base-file reader
    private val baseFileReaderAvroSchema = new Schema.Parser().parse(baseFileReaderSchema.avroSchemaStr)
    private val requiredSchemaFieldOrdinals: List[Int] = collectFieldOrdinals(requiredAvroSchema, baseFileReaderAvroSchema)

    private val serializer = sparkAdapter.createAvroSerializer(baseFileReaderSchema.structTypeSchema,
      baseFileReaderAvroSchema, resolveAvroSchemaNullability(baseFileReaderAvroSchema))

    private val recordKeyOrdinal = baseFileReaderSchema.structTypeSchema.fieldIndex(tableState.recordKeyField)

    override def hasNext: Boolean = hasNextInternal

    // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
    //       that recursion is unfolded into a loop to avoid stack overflows while
    //       handling records
    @tailrec private def hasNextInternal: Boolean = {
      if (baseFileIterator.hasNext) {
        val curRowRecord = baseFileIterator.next()
        val curKey = curRowRecord.getString(recordKeyOrdinal)
        val updatedRecordOpt = removeLogRecord(curKey)
        if (updatedRecordOpt.isEmpty) {
          // No merge needed, load current row with required projected schema
          recordToLoad = unsafeProjection(projectRowUnsafe(curRowRecord, requiredSchema.structTypeSchema, requiredSchemaFieldOrdinals))
          true
        } else {
          val mergedAvroRecordOpt = merge(serialize(curRowRecord), updatedRecordOpt.get)
          if (mergedAvroRecordOpt.isEmpty) {
            // Record has been deleted, skipping
            this.hasNextInternal
          } else {
            // NOTE: In occurrence of a merge we can't know the schema of the record being returned, b/c
            //       record from the Delta Log will bear (full) Table schema, while record from the Base file
            //       might already be read in projected one (as an optimization).
            //       As such we can't use more performant [[projectAvroUnsafe]], and instead have to fallback
            //       to [[projectAvro]]
            val projectedAvroRecord = projectAvro(mergedAvroRecordOpt.get, requiredAvroSchema, recordBuilder)
            recordToLoad = unsafeProjection(deserialize(projectedAvroRecord))
            true
          }
        }
      } else {
        super[LogFileIterator].hasNext
      }
    }

    private def serialize(curRowRecord: InternalRow): GenericRecord =
      serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]

    private def merge(curAvroRecord: GenericRecord, newRecord: HoodieRecord[_ <: HoodieRecordPayload[_]]): Option[IndexedRecord] = {
      // NOTE: We have to pass in Avro Schema used to read from Delta Log file since we invoke combining API
      //       on the record from the Delta Log
      toScalaOption(newRecord.getData.combineAndGetUpdateValue(curAvroRecord, logFileReaderAvroSchema, payloadProps))
    }
  }
}

private object HoodieMergeOnReadRDD {

  val CONFIG_INSTANTIATION_LOCK = new Object()

  def scanLog(logFiles: List[HoodieLogFile],
              partitionPath: Path,
              logSchema: Schema,
              tableState: HoodieTableState,
              maxCompactionMemoryInBytes: Long,
              hadoopConf: Configuration, internalSchema: InternalSchema = InternalSchema.getEmptyInternalSchema): HoodieMergedLogRecordScanner = {
    val tablePath = tableState.tablePath
    val fs = FSUtils.getFs(tablePath, hadoopConf)

    if (HoodieTableMetadata.isMetadataTable(tablePath)) {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(tableState.metadataConfig.getProps).enable(true).build()
      val dataTableBasePath = getDataTableBasePathFromMetadataTable(tablePath)
      val metadataTable = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(hadoopConf), metadataConfig,
        dataTableBasePath,
        hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))

      // We have to force full-scan for the MT log record reader, to make sure
      // we can iterate over all of the partitions, since by default some of the partitions (Column Stats,
      // Bloom Filter) are in "point-lookup" mode
      val forceFullScan = true

      // NOTE: In case of Metadata Table partition path equates to partition name (since there's just one level
      //       of indirection among MT partitions)
      val relativePartitionPath = getRelativePartitionPath(new Path(tablePath), partitionPath)
      metadataTable.getLogRecordScanner(logFiles.asJava, relativePartitionPath, toJavaOption(Some(forceFullScan)))
        .getLeft
    } else {
      val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(tablePath)
        .withLogFilePaths(logFiles.map(logFile => logFile.getPath.toString).asJava)
        .withReaderSchema(logSchema)
        .withLatestInstantTime(tableState.latestCommitTimestamp)
        .withReadBlocksLazily(
          Try(hadoopConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean)
            .getOrElse(false))
        .withReverseReader(false)
        .withInternalSchema(internalSchema)
        .withBufferSize(
          hadoopConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
            HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withMaxMemorySizeInBytes(maxCompactionMemoryInBytes)
        .withSpillableMapBasePath(
          hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
            HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(
          hadoopConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key,
            HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue))
        .withBitCaskDiskMapCompressionEnabled(
          hadoopConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
          HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
      if (logFiles.nonEmpty) {
        logRecordScannerBuilder.withPartition(
          getRelativePartitionPath(new Path(tableState.tablePath), logFiles.head.getPath.getParent))
      }

      logRecordScannerBuilder.build()
    }
  }

  /**
   * Projects provided instance of [[InternalRow]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   */
  private def projectRowUnsafe(row: InternalRow,
                         projectedSchema: StructType,
                         ordinals: Seq[Int]): InternalRow = {
    val projectedRow = new SpecificInternalRow(projectedSchema)
    var curIndex = 0
    projectedSchema.zip(ordinals).foreach { case (field, pos) =>
      val curField = if (row.isNullAt(pos)) {
        null
      } else {
        row.get(pos, field.dataType)
      }
      projectedRow.update(curIndex, curField)
      curIndex += 1
    }
    projectedRow
  }

  /**
   * Projects provided instance of [[IndexedRecord]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   */
  def projectAvroUnsafe(record: IndexedRecord,
                        projectedSchema: Schema,
                        ordinals: List[Int],
                        recordBuilder: GenericRecordBuilder): GenericRecord = {
    val fields = projectedSchema.getFields.asScala
    checkState(fields.length == ordinals.length)
    fields.zip(ordinals).foreach {
      case (field, pos) => recordBuilder.set(field, record.get(pos))
    }
    recordBuilder.build()
  }

  /**
   * Projects provided instance of [[IndexedRecord]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   *
   * This is a "safe" counterpart of [[projectAvroUnsafe]]: it does build mapping of the record's
   * schema into projected one itself (instead of expecting such mapping from the caller)
   */
  def projectAvro(record: IndexedRecord,
                  projectedSchema: Schema,
                  recordBuilder: GenericRecordBuilder): GenericRecord = {
    projectAvroUnsafe(record, projectedSchema, collectFieldOrdinals(projectedSchema, record.getSchema), recordBuilder)
  }

  /**
   * Maps [[projected]] [[Schema]] onto [[source]] one, collecting corresponding field ordinals w/in it, which
   * will be subsequently used by either [[projectRowUnsafe]] or [[projectAvroUnsafe()]] method
   *
   * @param projected target projected schema (which is a proper subset of [[source]] [[Schema]])
   * @param source source schema of the record being projected
   * @return list of ordinals of corresponding fields of [[projected]] schema w/in [[source]] one
   */
  private def collectFieldOrdinals(projected: Schema, source: Schema): List[Int] = {
    projected.getFields.asScala.map(f => source.getField(f.name()).pos()).toList
  }

  private def getPartitionPath(split: HoodieMergeOnReadFileSplit): Path = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile => new Path(baseFile.filePath))
      .getOrElse(split.logFiles.head.getPath)
      .getParent
  }

  private def resolveAvroSchemaNullability(schema: Schema) = {
    AvroConversionUtils.resolveAvroTypeNullability(schema) match {
      case (nullable, _) => nullable
    }
  }

  trait AvroDeserializerSupport extends SparkAdapterSupport {
    protected val requiredAvroSchema: Schema
    protected val requiredStructTypeSchema: StructType

    private lazy val deserializer: HoodieAvroDeserializer =
      sparkAdapter.createAvroDeserializer(requiredAvroSchema, requiredStructTypeSchema)

    protected def deserialize(avroRecord: GenericRecord): InternalRow = {
      checkState(avroRecord.getSchema.getFields.size() == requiredStructTypeSchema.fields.length)
      deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
    }
  }
}
