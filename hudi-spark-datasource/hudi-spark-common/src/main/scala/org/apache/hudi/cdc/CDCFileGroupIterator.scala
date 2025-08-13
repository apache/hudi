/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cdc

import org.apache.hudi.{AvroConversionUtils, HoodieTableSchema, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMemoryConfig, HoodieMetadataConfig, HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.config.HoodieCommonConfig.{DISK_MAP_BITCASK_COMPRESSION_ENABLED, SPILLABLE_DISK_MAP_TYPE}
import org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, HoodieRecordMerger}
import org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID
import org.apache.hudi.common.serialization.DefaultSerializer
import org.apache.hudi.common.table.{HoodieTableMetaClient, PartialUpdateMode}
import org.apache.hudi.common.table.cdc.{HoodieCDCFileSplit, HoodieCDCUtils}
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase._
import org.apache.hudi.common.table.cdc.HoodieCDCOperation._
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode._
import org.apache.hudi.common.table.log.{HoodieCDCLogRecordIterator, HoodieMergedLogRecordReader}
import org.apache.hudi.common.table.read.{BufferedRecord, BufferedRecordMerger, BufferedRecordMergerFactory, BufferedRecords, FileGroupReaderSchemaHandler, HoodieFileGroupReader, HoodieReadStats, IteratorMode, UpdateProcessor}
import org.apache.hudi.common.table.read.buffer.KeyBasedFileGroupRecordBuffer
import org.apache.hudi.common.util.{DefaultSizeEstimator, FileIOUtils, HoodieRecordUtils, Option}
import org.apache.hudi.common.util.collection.ExternalSpillableMap
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util
import java.util.{Collections, Locale}
import java.util.stream.Collectors

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

class CDCFileGroupIterator(split: HoodieCDCFileGroupSplit,
                           metaClient: HoodieTableMetaClient,
                           conf: StorageConfiguration[Configuration],
                           baseFileReader: SparkColumnarFileReader,
                           originTableSchema: HoodieTableSchema,
                           cdcSchema: StructType,
                           requiredCdcSchema: StructType,
                           props: TypedProperties)
  extends Iterator[InternalRow]
  with SparkAdapterSupport with AvroDeserializerSupport with Closeable {

  private lazy val readerContext = {
    val bufferedReaderContext = new SparkFileFormatInternalRowReaderContext(baseFileReader,
      Seq.empty, Seq.empty, conf, metaClient.getTableConfig)
    bufferedReaderContext.initRecordMerger(readerProperties)
    bufferedReaderContext
  }

  private lazy val orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode, props, metaClient)
  private lazy val payloadClass: Option[String] = if (recordMerger.getMergingStrategy == PAYLOAD_BASED_MERGE_STRATEGY_UUID) {
    Option.of(metaClient.getTableConfig.getPayloadClass)
  } else {
    Option.empty.asInstanceOf[Option[String]]
  }
  private lazy val partialUpdateMode: PartialUpdateMode = metaClient.getTableConfig.getPartialUpdateMode
  private var isPartialMergeEnabled = false
  private var bufferedRecordMerger = getBufferedRecordMerger
  private def getBufferedRecordMerger: BufferedRecordMerger[InternalRow] = BufferedRecordMergerFactory.create(readerContext,
    readerContext.getMergeMode, isPartialMergeEnabled, Option.of(recordMerger), orderingFieldNames,
    payloadClass, avroSchema, props, partialUpdateMode)

  private lazy val storage = metaClient.getStorage

  private lazy val basePath = metaClient.getBasePath

  private lazy val readerProperties: TypedProperties = {
    val configuration = conf.unwrapAs(classOf[Configuration])
    val readerProps = TypedProperties.copy(props)
    readerProps.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key,
      configuration.getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(),
        HoodieMemoryConfig.DEFAULT_MR_MAX_DFS_STREAM_BUFFER_SIZE).toString)
    readerProps.setProperty(SPILLABLE_MAP_BASE_PATH.key,
      configuration.get(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key,
        FileIOUtils.getDefaultSpillableMapBasePath))
    readerProps.setProperty(SPILLABLE_DISK_MAP_TYPE.key,
      configuration.get(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key,
        HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue.toString))
    readerProps.setProperty(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key,
      configuration.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()).toString)
    readerProps.setProperty(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.key,
      configuration.get(HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.key(),
        HoodieReaderConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN.defaultValue()))
    readerProps
  }

  private lazy val recordMerger: HoodieRecordMerger = readerContext.getRecordMerger().get()

  protected override val avroSchema: Schema = new Schema.Parser().parse(originTableSchema.avroSchemaStr)

  protected override val structTypeSchema: StructType = originTableSchema.structTypeSchema

  private val cdcSupplementalLoggingMode = metaClient.getTableConfig.cdcSupplementalLoggingMode

  private lazy val cdcAvroSchema: Schema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
    cdcSupplementalLoggingMode,
    HoodieAvroUtils.removeMetadataFields(avroSchema)
  )

  private lazy val cdcSparkSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(cdcAvroSchema)

  private lazy val sparkPartitionedFileUtils = sparkAdapter.getSparkPartitionedFileUtils

  /**
   * The deserializer used to convert the CDC GenericRecord to Spark InternalRow.
   */
  private lazy val cdcRecordDeserializer: HoodieAvroDeserializer = {
    sparkAdapter.createAvroDeserializer(cdcAvroSchema, cdcSparkSchema)
  }

  private lazy val projection: Projection = generateUnsafeProjection(cdcSchema, requiredCdcSchema)

  // Iterator on cdc file
  private val cdcFileIter = split.changes.iterator

  // The instant that is currently being processed
  private var currentInstant: String = _

  // The change file that is currently being processed
  private var currentCDCFileSplit: HoodieCDCFileSplit = _

  /**
   * Two cases will use this to iterator the records:
   * 1) extract the change data from the base file directly, including 'BASE_FILE_INSERT' and 'BASE_FILE_DELETE'.
   * 2) when the type of cdc file is 'REPLACE_COMMIT',
   * use this to trace the records that are converted from the '[[beforeImageRecords]]
   */
  private var recordIter: Iterator[BufferedRecord[InternalRow]] = Iterator.empty

  /**
   * Only one case where it will be used is that extract the change data from log files for mor table.
   * At the time, 'logRecordIter' will work with [[beforeImageRecords]] that keep all the records of the previous file slice.
   */
  private var logRecordIter: Iterator[BufferedRecord[InternalRow]] = Iterator.empty
  private var keyBasedFileGroupRecordBuffer: Option[KeyBasedFileGroupRecordBuffer[InternalRow]] = Option.empty.asInstanceOf[Option[KeyBasedFileGroupRecordBuffer[InternalRow]]]

  /**
   * Only one case where it will be used is that extract the change data from cdc log files.
   */
  private var cdcLogRecordIterator: HoodieCDCLogRecordIterator = _

  /**
   * The next record need to be returned when call next().
   */
  protected var recordToLoad: InternalRow = _

  /**
   * The list of files to which 'beforeImageRecords' belong.
   * Use it to determine if 'beforeImageRecords' contains all the required data that extract
   * the change data from the current cdc file.
   */
  private val beforeImageFiles: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

  /**
   * Keep the before-image data. There cases will use this:
   * 1) the cdc infer case is [[LOG_FILE]];
   * 2) the cdc infer case is [[AS_IS]] and [[cdcSupplementalLoggingMode]] is 'op_key'.
   */
  private val beforeImageRecords: mutable.Map[String, BufferedRecord[InternalRow]] = mutable.Map.empty

  /**
   * Keep the after-image data. Only one case will use this:
   * the cdc infer case is [[AS_IS]] and [[cdcSupplementalLoggingMode]] is [[OP_KEY_ONLY]] or [[DATA_BEFORE]].
   */
  private val afterImageRecords: util.Map[String, BufferedRecord[InternalRow]] = new ExternalSpillableMap[String, BufferedRecord[InternalRow]](
    props.getLong(HoodieWriteConfig.CDC_FILE_GROUP_ITERATOR_MEMORY_SPILL_BYTES.key(),
      HoodieWriteConfig.CDC_FILE_GROUP_ITERATOR_MEMORY_SPILL_BYTES.defaultValue()),
    props.getString(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key, FileIOUtils.getDefaultSpillableMapBasePath),
    new DefaultSizeEstimator[String],
    new DefaultSizeEstimator[BufferedRecord[InternalRow]],
    ExternalSpillableMap.DiskMapType.valueOf(props.getString(
      SPILLABLE_DISK_MAP_TYPE.key(), SPILLABLE_DISK_MAP_TYPE.defaultValue().toString)
      .toUpperCase(Locale.ROOT)),
    new DefaultSerializer[BufferedRecord[InternalRow]],
    props.getBoolean(DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()),
    getClass.getSimpleName)

  private val internalRowToJsonStringConverterMap: mutable.Map[Integer, InternalRowToJsonStringConverter] = mutable.Map.empty

  private def needLoadNextFile: Boolean = {
    !recordIter.hasNext &&
      !logRecordIter.hasNext &&
      (cdcLogRecordIterator == null || !cdcLogRecordIterator.hasNext)
  }

  @tailrec final def hasNextInternal: Boolean = {
    if (needLoadNextFile) {
      loadCdcFile()
    }
    if (currentCDCFileSplit == null) {
      false
    } else {
      currentCDCFileSplit.getCdcInferCase match {
        case BASE_FILE_INSERT | BASE_FILE_DELETE | REPLACE_COMMIT =>
          if (recordIter.hasNext && loadNext()) {
            true
          } else {
            hasNextInternal
          }
        case LOG_FILE =>
          if (logRecordIter.hasNext && loadNext()) {
            true
          } else {
            hasNextInternal
          }
        case AS_IS =>
          if (cdcLogRecordIterator.hasNext && loadNext()) {
            true
          } else {
            hasNextInternal
          }
      }
    }
  }

  override def hasNext: Boolean = hasNextInternal

  override final def next(): InternalRow = {
    projection(recordToLoad)
  }

  def loadNext(): Boolean = {
    var loaded = false
    currentCDCFileSplit.getCdcInferCase match {
      case BASE_FILE_INSERT =>
        val originRecord = recordIter.next()
        recordToLoad.update(3, convertBufferedRecordToJsonString(originRecord))
        loaded = true
      case BASE_FILE_DELETE =>
        val originRecord = recordIter.next()
        recordToLoad.update(2, convertBufferedRecordToJsonString(originRecord))
        loaded = true
      case LOG_FILE =>
        loaded = loadNextLogRecord()
      case AS_IS =>
        val record = cdcLogRecordIterator.next().asInstanceOf[GenericRecord]
        cdcSupplementalLoggingMode match {
          case `DATA_BEFORE_AFTER` =>
            recordToLoad.update(0, convertToUTF8String(String.valueOf(record.get(0))))
            val before = record.get(2).asInstanceOf[GenericRecord]
            recordToLoad.update(2, recordToJsonAsUTF8String(before))
            val after = record.get(3).asInstanceOf[GenericRecord]
            recordToLoad.update(3, recordToJsonAsUTF8String(after))
          case `DATA_BEFORE` =>
            val row = cdcRecordDeserializer.deserialize(record).get.asInstanceOf[InternalRow]
            val op = row.getString(0)
            val recordKey = row.getString(1)
            recordToLoad.update(0, convertToUTF8String(op))
            val before = record.get(2).asInstanceOf[GenericRecord]
            recordToLoad.update(2, recordToJsonAsUTF8String(before))
            parse(op) match {
              case INSERT =>
                recordToLoad.update(3, convertBufferedRecordToJsonString(afterImageRecords.get(recordKey)))
              case UPDATE =>
                recordToLoad.update(3, convertBufferedRecordToJsonString(afterImageRecords.get(recordKey)))
              case _ =>
                recordToLoad.update(3, null)
            }
          case _ =>
            val row = cdcRecordDeserializer.deserialize(record).get.asInstanceOf[InternalRow]
            val op = row.getString(0)
            val recordKey = row.getString(1)
            recordToLoad.update(0, convertToUTF8String(op))
            parse(op) match {
              case INSERT =>
                recordToLoad.update(2, null)
                recordToLoad.update(3, convertBufferedRecordToJsonString(afterImageRecords.get(recordKey)))
              case UPDATE =>
                recordToLoad.update(2, convertBufferedRecordToJsonString(beforeImageRecords(recordKey)))
                recordToLoad.update(3, convertBufferedRecordToJsonString(afterImageRecords.get(recordKey)))
              case _ =>
                recordToLoad.update(2, convertBufferedRecordToJsonString(beforeImageRecords(recordKey)))
                recordToLoad.update(3, null)
            }
        }
        loaded = true
      case REPLACE_COMMIT =>
        val originRecord = recordIter.next()
        recordToLoad.update(2, convertBufferedRecordToJsonString(originRecord))
        loaded = true
    }
    loaded
  }

  /**
   * Load the next log record, and judge how to convert it to cdc format.
   */
  private def loadNextLogRecord(): Boolean = {
    var loaded = false
    val logRecord = logRecordIter.next()
    if (logRecord.isDelete) {
      // it's a deleted record.
      val existingRecordOpt = beforeImageRecords.remove(logRecord.getRecordKey)
      if (existingRecordOpt.isEmpty) {
        // no real record is deleted, just ignore.
      } else {
        // there is a real record deleted.
        recordToLoad.update(0, CDCRelation.CDC_OPERATION_DELETE)
        recordToLoad.update(2, convertBufferedRecordToJsonString(existingRecordOpt.get))
        recordToLoad.update(3, null)
        loaded = true
      }
    } else {
      val existingRecordOpt = beforeImageRecords.get(logRecord.getRecordKey)
      if (existingRecordOpt.isEmpty) {
        // a new record is inserted.
        recordToLoad.update(0, CDCRelation.CDC_OPERATION_INSERT)
        recordToLoad.update(2, null)
        recordToLoad.update(3, convertBufferedRecordToJsonString(logRecord))
        // insert into beforeImageRecords
        beforeImageRecords(logRecord.getRecordKey) = logRecord
        loaded = true
      } else {
        // a existed record is updated.
        val existingRecord = existingRecordOpt.get
        val mergeRecord = merge(existingRecord, logRecord)
        if (existingRecord != mergeRecord) {
          recordToLoad.update(0, CDCRelation.CDC_OPERATION_UPDATE)
          recordToLoad.update(2, convertBufferedRecordToJsonString(existingRecord))
          recordToLoad.update(3, convertBufferedRecordToJsonString(mergeRecord))
          // update into beforeImageRecords
          beforeImageRecords(logRecord.getRecordKey) = mergeRecord
          loaded = true
        }
      }
    }
    loaded
  }

  private def loadCdcFile(): Unit = {
    // reset all the iterator first.
    recordIter = Iterator.empty
    logRecordIter = Iterator.empty
    keyBasedFileGroupRecordBuffer.ifPresent(k => k.close())
    keyBasedFileGroupRecordBuffer = Option.empty.asInstanceOf[Option[KeyBasedFileGroupRecordBuffer[InternalRow]]]
    beforeImageRecords.clear()
    afterImageRecords.clear()
    if (cdcLogRecordIterator != null) {
      cdcLogRecordIterator.close()
      cdcLogRecordIterator = null
    }

    if (cdcFileIter.hasNext) {
      val split = cdcFileIter.next()
      currentInstant = split.getInstant
      currentCDCFileSplit = split
      currentCDCFileSplit.getCdcInferCase match {
        case BASE_FILE_INSERT =>
          assert(currentCDCFileSplit.getCdcFiles != null && currentCDCFileSplit.getCdcFiles.size() == 1)
          val absCDCPath = new StoragePath(basePath, currentCDCFileSplit.getCdcFiles.get(0))
          val fileStatus = storage.getPathInfo(absCDCPath)

          val pf = sparkPartitionedFileUtils.createPartitionedFile(
            InternalRow.empty, absCDCPath, 0, fileStatus.getLength)
          recordIter = baseFileReader.read(pf, originTableSchema.structTypeSchema, new StructType(),
            toJavaOption(originTableSchema.internalSchema), Seq.empty, conf)
            .map(record => BufferedRecords.fromEngineRecord(record, avroSchema, readerContext.getRecordContext, orderingFieldNames, false))
        case BASE_FILE_DELETE =>
          assert(currentCDCFileSplit.getBeforeFileSlice.isPresent)
          recordIter = loadFileSlice(currentCDCFileSplit.getBeforeFileSlice.get)
        case LOG_FILE =>
          assert(currentCDCFileSplit.getCdcFiles != null && currentCDCFileSplit.getCdcFiles.size() == 1
            && currentCDCFileSplit.getBeforeFileSlice.isPresent)
          loadBeforeFileSliceIfNeeded(currentCDCFileSplit.getBeforeFileSlice.get)
          val absLogPath = new StoragePath(basePath, currentCDCFileSplit.getCdcFiles.get(0))
          val logFile = new HoodieLogFile(storage.getPathInfo(absLogPath))
          logRecordIter = loadLogFile(logFile, split.getInstant)
        case AS_IS =>
          assert(currentCDCFileSplit.getCdcFiles != null && !currentCDCFileSplit.getCdcFiles.isEmpty)
          // load beforeFileSlice to beforeImageRecords
          if (currentCDCFileSplit.getBeforeFileSlice.isPresent) {
            loadBeforeFileSliceIfNeeded(currentCDCFileSplit.getBeforeFileSlice.get)
          }
          // load afterFileSlice to afterImageRecords
          if (currentCDCFileSplit.getAfterFileSlice.isPresent) {
            val iter = loadFileSlice(currentCDCFileSplit.getAfterFileSlice.get())
            afterImageRecords.clear()
            iter.foreach { bufferedRecord =>
              afterImageRecords.put(bufferedRecord.getRecordKey, bufferedRecord)
            }
          }

          val cdcLogFiles = currentCDCFileSplit.getCdcFiles.asScala.map { cdcFile =>
            new HoodieLogFile(storage.getPathInfo(new StoragePath(basePath, cdcFile)))
          }.toArray
          cdcLogRecordIterator = new HoodieCDCLogRecordIterator(storage, cdcLogFiles, cdcAvroSchema)
        case REPLACE_COMMIT =>
          if (currentCDCFileSplit.getBeforeFileSlice.isPresent) {
            loadBeforeFileSliceIfNeeded(currentCDCFileSplit.getBeforeFileSlice.get)
          }
          recordIter = beforeImageRecords.values.iterator
      }
      resetRecordFormat()
    } else {
      currentInstant = null
      currentCDCFileSplit = null
    }
  }

  /**
   * Initialize the partial fields of the data to be returned in advance to speed up.
   */
  private def resetRecordFormat(): Unit = {
    recordToLoad = currentCDCFileSplit.getCdcInferCase match {
      case BASE_FILE_INSERT =>
        InternalRow.fromSeq(Seq(
          CDCRelation.CDC_OPERATION_INSERT, convertToUTF8String(currentInstant),
          null, null))
      case BASE_FILE_DELETE =>
        InternalRow.fromSeq(Seq(
          CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant),
          null, null))
      case LOG_FILE =>
        InternalRow.fromSeq(Seq(
          null, convertToUTF8String(currentInstant),
          null, null))
      case AS_IS =>
        InternalRow.fromSeq(Seq(
          null, convertToUTF8String(currentInstant),
          null, null))
      case REPLACE_COMMIT =>
        InternalRow.fromSeq(Seq(
          CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant),
          null, null))
    }
  }

  /**
   * If [[beforeImageFiles]] are the list of file that we want to load exactly, use this directly.
   * Otherwise we need to re-load what we need.
   */
  private def loadBeforeFileSliceIfNeeded(fileSlice: FileSlice): Unit = {
    val files = List(fileSlice.getBaseFile.get().getPath) ++
      fileSlice.getLogFiles.collect(Collectors.toList[HoodieLogFile]).asScala
        .map(f => f.getPath.toUri.toString).toList
    val same = files.sorted == beforeImageFiles.sorted.toList
    if (!same) {
      // clear up the beforeImageRecords
      beforeImageRecords.clear()
      val iter = loadFileSlice(fileSlice)
      iter.foreach { bufferedRecord =>
        beforeImageRecords.put(bufferedRecord.getRecordKey, bufferedRecord)
      }
      // reset beforeImageFiles
      beforeImageFiles.clear()
      beforeImageFiles.append(files: _*)
    }
  }

  private def loadFileSlice(fileSlice: FileSlice): Iterator[BufferedRecord[InternalRow]] = {
    loadFileSlice(fileSlice, readerContext)
  }

  private def loadFileSlice(fileSlice: FileSlice, readerContext: SparkFileFormatInternalRowReaderContext): Iterator[BufferedRecord[InternalRow]] = {
    val fileGroupReader = HoodieFileGroupReader.newBuilder()
      .withReaderContext(readerContext)
      .withHoodieTableMetaClient(metaClient)
      .withFileSlice(fileSlice)
      .withDataSchema(avroSchema)
      .withRequestedSchema(avroSchema)
      .withInternalSchema(toJavaOption(originTableSchema.internalSchema))
      .withProps(readerProperties)
      .withLatestCommitTime(split.changes.last.getInstant)
      .build()
    CloseableIteratorListener.addListener(fileGroupReader.getClosableBufferedRecordIterator).asScala
  }

  private def loadLogFile(logFile: HoodieLogFile, instant: String): Iterator[BufferedRecord[InternalRow]] = {
    val partitionPath = FSUtils.getRelativePartitionPath(metaClient.getBasePath, logFile.getPath.getParent)
    readerContext.setLatestCommitTime(instant)
    readerContext.setHasBootstrapBaseFile(false)
    readerContext.setHasLogFiles(true)
    readerContext.setSchemaHandler(
      new FileGroupReaderSchemaHandler[InternalRow](readerContext, avroSchema, avroSchema,
        Option.empty(), metaClient.getTableConfig, readerProperties))
    val stats = new HoodieReadStats
    keyBasedFileGroupRecordBuffer.ifPresent(k => k.close())
    keyBasedFileGroupRecordBuffer = Option.of(new KeyBasedFileGroupRecordBuffer[InternalRow](readerContext, metaClient, readerContext.getMergeMode,
      metaClient.getTableConfig.getPartialUpdateMode, readerProperties, metaClient.getTableConfig.getPreCombineFields,
      UpdateProcessor.create(stats, readerContext, true, Option.empty())))

    HoodieMergedLogRecordReader.newBuilder[InternalRow]
      .withStorage(metaClient.getStorage)
      .withHoodieReaderContext(readerContext)
      .withLogFiles(Collections.singletonList(logFile))
      .withReverseReader(false)
      .withBufferSize(HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue)
      .withPartition(partitionPath)
      .withMetaClient(metaClient)
      .withRecordBuffer(keyBasedFileGroupRecordBuffer.get())
      .build

    CloseableIteratorListener.addListener(keyBasedFileGroupRecordBuffer.get().getLogRecordIterator).asScala
  }

  /**
   * Convert InternalRow to json string.
   */
  private def convertBufferedRecordToJsonString(record: BufferedRecord[InternalRow]): UTF8String = {
    internalRowToJsonStringConverterMap.getOrElseUpdate(record.getSchemaId,
      new InternalRowToJsonStringConverter(HoodieInternalRowUtils.getCachedSchema(readerContext.getRecordContext.decodeAvroSchema(record.getSchemaId))))
      .convert(record.getRecord)
  }

  /**
   * The data of string type is stored in InternalRow using UTF8String type.
   */
  private def convertToUTF8String(str: String): UTF8String = {
    UTF8String.fromString(str)
  }

  private def recordToJsonAsUTF8String(record: GenericRecord): UTF8String = {
    convertToUTF8String(HoodieCDCUtils.recordToJson(record))
  }

  private def merge(currentRecord: BufferedRecord[InternalRow], newRecord: BufferedRecord[InternalRow]): BufferedRecord[InternalRow] = {
    if (!isPartialMergeEnabled && keyBasedFileGroupRecordBuffer.isPresent && keyBasedFileGroupRecordBuffer.get().isPartialMergingEnabled) {
      isPartialMergeEnabled = true
      bufferedRecordMerger = getBufferedRecordMerger
    }
    val deltaMergeResult = bufferedRecordMerger.deltaMerge(newRecord, currentRecord)
    if (deltaMergeResult.isEmpty) {
      currentRecord
    } else {
      deltaMergeResult.get()
    }
  }

  override def close(): Unit = {
    recordIter = Iterator.empty
    logRecordIter = Iterator.empty
    keyBasedFileGroupRecordBuffer.ifPresent(k => k.close())
    keyBasedFileGroupRecordBuffer = Option.empty.asInstanceOf[Option[KeyBasedFileGroupRecordBuffer[InternalRow]]]
    beforeImageRecords.clear()
    afterImageRecords.clear()
    if (cdcLogRecordIterator != null) {
      cdcLogRecordIterator.close()
      cdcLogRecordIterator = null
    }
  }
}
