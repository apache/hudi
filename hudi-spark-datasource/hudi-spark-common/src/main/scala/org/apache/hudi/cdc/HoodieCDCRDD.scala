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

package org.apache.hudi.cdc

import org.apache.hudi.{AvroConversionUtils, AvroProjection, HoodieFileIndex, HoodieMergeOnReadFileSplit, HoodieTableSchema, HoodieTableState, HoodieUnsafeRDD, LogFileIterator, RecordMergingFileIterator, SparkAdapterSupport}
import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieConversionUtils._
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.{HoodieCDCFileSplit, HoodieCDCUtils}
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase._
import org.apache.hudi.common.table.cdc.HoodieCDCOperation._
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode._
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.util.JFunction

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.hadoop.fs.Path
import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util.Properties
import java.util.stream.Collectors

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The split that will be processed by spark task.
 * The [[changes]] should be sorted first.
 */
case class HoodieCDCFileGroupSplit(changes: Array[HoodieCDCFileSplit])

/**
 * The Spark [[Partition]]'s implementation.
 */
case class HoodieCDCFileGroupPartition(
    index: Int,
    split: HoodieCDCFileGroupSplit
) extends Partition

class HoodieCDCRDD(
    spark: SparkSession,
    metaClient: HoodieTableMetaClient,
    parquetReader: PartitionedFile => Iterator[InternalRow],
    originTableSchema: HoodieTableSchema,
    cdcSchema: StructType,
    requiredCdcSchema: StructType,
    @transient changes: Array[HoodieCDCFileGroupSplit])
  extends RDD[InternalRow](spark.sparkContext, Nil) with HoodieUnsafeRDD {

  @transient
  private val hadoopConf = spark.sparkContext.hadoopConfiguration

  private val confBroadcast = spark.sparkContext.broadcast(new SerializableWritable(hadoopConf))

  private val cdcSupplementalLoggingMode = metaClient.getTableConfig.cdcSupplementalLoggingMode

  private val props = HoodieFileIndex.getConfigProperties(spark, Map.empty, metaClient.getTableConfig)

  protected val payloadProps: Properties = metaClient.getTableConfig.getOrderingFieldsStr
    .map[TypedProperties](JFunction.toJavaFunction(preCombineFields =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingFields(preCombineFields)
        .build
        .getProps
    )).orElse(new TypedProperties())

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val cdcPartition = split.asInstanceOf[HoodieCDCFileGroupPartition]
    new CDCFileGroupIterator(cdcPartition.split, metaClient)
  }

  override protected def getPartitions: Array[Partition] = {
    changes.zipWithIndex.map{ case (split, index) =>
      HoodieCDCFileGroupPartition(index, split)
    }.toArray
  }

  private class CDCFileGroupIterator(
      split: HoodieCDCFileGroupSplit,
      metaClient: HoodieTableMetaClient
    ) extends Iterator[InternalRow] with SparkAdapterSupport with AvroDeserializerSupport with Closeable {

    private lazy val storage = metaClient.getStorage

    private lazy val conf = confBroadcast.value.value

    private lazy val basePath = metaClient.getBasePath

    private lazy val tableConfig = metaClient.getTableConfig

    private lazy val populateMetaFields = tableConfig.populateMetaFields()

    private lazy val keyGenerator = {
      HoodieSparkKeyGeneratorFactory.createKeyGenerator(tableConfig.getProps())
    }

    private lazy val recordKeyField: String = if (populateMetaFields) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = metaClient.getTableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

    private lazy val orderingFields: List[String] = metaClient.getTableConfig.getOrderingFields.asScala.toList

    private lazy val tableState = {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build()
      HoodieTableState(
        basePath.toUri.toString,
        Some(split.changes.last.getInstant),
        recordKeyField,
        orderingFields,
        usesVirtualKeys = !populateMetaFields,
        metaClient.getTableConfig.getPayloadClass,
        metadataConfig,
        // TODO support CDC with spark record
        recordMergeImplClasses = List(classOf[HoodieAvroRecordMerger].getName),
        recordMergeStrategyId = HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID
      )
    }

    protected override val avroSchema: Schema = new Schema.Parser().parse(originTableSchema.avroSchemaStr)

    protected override val structTypeSchema: StructType = originTableSchema.structTypeSchema

    private lazy val serializer = sparkAdapter.createAvroSerializer(originTableSchema.structTypeSchema,
      avroSchema, nullable = false)

    private lazy val avroProjection = AvroProjection.create(avroSchema)

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
     *    use this to trace the records that are converted from the '[[beforeImageRecords]]
     */
    private var recordIter: Iterator[InternalRow] = Iterator.empty

    /**
     * Only one case where it will be used is that extract the change data from log files for mor table.
     * At the time, 'logRecordIter' will work with [[beforeImageRecords]] that keep all the records of the previous file slice.
     */
    private var logRecordIter: Iterator[(String, HoodieRecord[_])] = Iterator.empty

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
    private var beforeImageRecords: mutable.Map[String, GenericRecord] = mutable.Map.empty

    /**
     * Keep the after-image data. Only one case will use this:
     * the cdc infer case is [[AS_IS]] and [[cdcSupplementalLoggingMode]] is [[OP_KEY_ONLY]] or [[DATA_BEFORE]].
     */
    private var afterImageRecords: mutable.Map[String, InternalRow] = mutable.Map.empty

    private var internalRowToJsonStringConverter = new InternalRowToJsonStringConverter(originTableSchema.structTypeSchema)

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
          recordToLoad.update(3, convertRowToJsonString(originRecord))
          loaded = true
        case BASE_FILE_DELETE =>
          val originRecord = recordIter.next()
          recordToLoad.update(2, convertRowToJsonString(originRecord))
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
                  recordToLoad.update(3, convertRowToJsonString(afterImageRecords(recordKey)))
                case UPDATE =>
                  recordToLoad.update(3, convertRowToJsonString(afterImageRecords(recordKey)))
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
                  recordToLoad.update(3, convertRowToJsonString(afterImageRecords(recordKey)))
                case UPDATE =>
                  recordToLoad.update(2, recordToJsonAsUTF8String(beforeImageRecords(recordKey)))
                  recordToLoad.update(3, convertRowToJsonString(afterImageRecords(recordKey)))
                case _ =>
                  recordToLoad.update(2, recordToJsonAsUTF8String(beforeImageRecords(recordKey)))
                  recordToLoad.update(3, null)
              }
          }
          loaded = true
        case REPLACE_COMMIT =>
          val originRecord = recordIter.next()
          recordToLoad.update(2, convertRowToJsonString(originRecord))
          loaded = true
      }
      loaded
    }

    /**
     * Load the next log record, and judge how to convert it to cdc format.
     */
    private def loadNextLogRecord(): Boolean = {
      var loaded = false
      val (key, logRecord) = logRecordIter.next()
      val indexedRecord = getInsertValue(logRecord)
      if (indexedRecord.isEmpty) {
        // it's a deleted record.
        val existingRecordOpt = beforeImageRecords.remove(key)
        if (existingRecordOpt.isEmpty) {
          // no real record is deleted, just ignore.
          logWarning("can not get any record that have the same key with the deleting logRecord.")
        } else {
          // there is a real record deleted.
          recordToLoad.update(0, CDCRelation.CDC_OPERATION_DELETE)
          recordToLoad.update(2, recordToJsonAsUTF8String(existingRecordOpt.get))
          recordToLoad.update(3, null)
          loaded = true
        }
      } else {
        val existingRecordOpt = beforeImageRecords.get(key)
        if (existingRecordOpt.isEmpty) {
          // a new record is inserted.
          val insertedRecord = avroProjection(indexedRecord.get.asInstanceOf[GenericRecord])
          recordToLoad.update(0, CDCRelation.CDC_OPERATION_INSERT)
          recordToLoad.update(2, null)
          recordToLoad.update(3, recordToJsonAsUTF8String(insertedRecord))
          // insert into beforeImageRecords
          beforeImageRecords(key) = insertedRecord
          loaded = true
        } else {
          // a existed record is updated.
          val existingRecord = existingRecordOpt.get
          val merged = merge(existingRecord, logRecord)
          val mergeRecord = avroProjection(merged.asInstanceOf[GenericRecord])
          if (existingRecord != mergeRecord) {
            recordToLoad.update(0, CDCRelation.CDC_OPERATION_UPDATE)
            recordToLoad.update(2, recordToJsonAsUTF8String(existingRecord))
            recordToLoad.update(3, recordToJsonAsUTF8String(mergeRecord))
            // update into beforeImageRecords
            beforeImageRecords(key) = mergeRecord
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
            val pathInfo = storage.getPathInfo(absCDCPath)

            val pf = sparkPartitionedFileUtils.createPartitionedFile(
              InternalRow.empty, absCDCPath, 0, pathInfo.getLength)
            recordIter = parquetReader(pf)
          case BASE_FILE_DELETE =>
            assert(currentCDCFileSplit.getBeforeFileSlice.isPresent)
            recordIter = loadFileSlice(currentCDCFileSplit.getBeforeFileSlice.get)
          case LOG_FILE =>
            assert(currentCDCFileSplit.getCdcFiles != null && currentCDCFileSplit.getCdcFiles.size() == 1
              && currentCDCFileSplit.getBeforeFileSlice.isPresent)
            loadBeforeFileSliceIfNeeded(currentCDCFileSplit.getBeforeFileSlice.get)
            val absLogPath = new StoragePath(basePath, currentCDCFileSplit.getCdcFiles.get(0))
            val morSplit = HoodieMergeOnReadFileSplit(None, List(new HoodieLogFile(storage.getPathInfo(absLogPath))))
            val logFileIterator = new LogFileIterator(morSplit, originTableSchema, originTableSchema, tableState, conf)
            logRecordIter = logFileIterator.logRecordsPairIterator
          case AS_IS =>
            assert(currentCDCFileSplit.getCdcFiles != null && !currentCDCFileSplit.getCdcFiles.isEmpty)
            // load beforeFileSlice to beforeImageRecords
            if (currentCDCFileSplit.getBeforeFileSlice.isPresent) {
              loadBeforeFileSliceIfNeeded(currentCDCFileSplit.getBeforeFileSlice.get)
            }
            // load afterFileSlice to afterImageRecords
            if (currentCDCFileSplit.getAfterFileSlice.isPresent) {
              val iter = loadFileSlice(currentCDCFileSplit.getAfterFileSlice.get())
              afterImageRecords = mutable.Map.empty
              iter.foreach { row =>
                val key = getRecordKey(row)
                afterImageRecords.put(key, row.copy())
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
            recordIter = beforeImageRecords.values.map { record =>
              deserialize(record)
            }.iterator
            beforeImageRecords.clear()
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
        iter.foreach { row =>
          val key = getRecordKey(row)
          // Due to the reuse buffer mechanism of Spark serialization,
          // we have to copy the serialized result if we need to retain its reference
          beforeImageRecords.put(key, serialize(row, copy = true))
        }
        // reset beforeImageFiles
        beforeImageFiles.clear()
        beforeImageFiles.append(files: _*)
      }
    }

    private def loadFileSlice(fileSlice: FileSlice): Iterator[InternalRow] = {
      val baseFileInfo = storage.getPathInfo(fileSlice.getBaseFile.get().getStoragePath)
      val basePartitionedFile = sparkPartitionedFileUtils.createPartitionedFile(
        InternalRow.empty,
        baseFileInfo.getPath,
        0,
        baseFileInfo.getLength
      )
      val logFiles = fileSlice.getLogFiles
        .sorted(HoodieLogFile.getLogFileComparator)
        .collect(Collectors.toList[HoodieLogFile])
        .asScala.toList
        .filterNot(_.getFileName.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))

      if (logFiles.isEmpty) {
        // no log files, just load the base parquet file
        parquetReader(basePartitionedFile)
      } else {
        // use [[RecordMergingFileIterator]] to load both the base file and log files
        val morSplit = HoodieMergeOnReadFileSplit(Some(basePartitionedFile), logFiles)
        new RecordMergingFileIterator(
          morSplit,
          BaseFileReader(parquetReader, originTableSchema.structTypeSchema),
          originTableSchema,
          originTableSchema,
          tableState,
          conf)
      }
    }

    /**
     * Convert InternalRow to json string.
     */
    private def convertRowToJsonString(record: InternalRow): UTF8String = {
      internalRowToJsonStringConverter.convert(record)
    }

    /**
     * The data of string type is stored in InternalRow using UTF8String type.
     */
    private def convertToUTF8String(str: String): UTF8String = {
      UTF8String.fromString(str)
    }

    private def pathToString(p: Path): String = {
      p.toUri.toString
    }

    private def serialize(curRowRecord: InternalRow, copy: Boolean = false): GenericRecord = {
      val record = serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]
      if (copy) {
        GenericData.get().deepCopy(record.getSchema, record)
      } else {
        record
      }
    }

    private def recordToJsonAsUTF8String(record: GenericRecord): UTF8String = {
      convertToUTF8String(HoodieCDCUtils.recordToJson(record))
    }

    private def getRecordKey(row: InternalRow): String = {
      if (populateMetaFields) {
        row.getString(structTypeSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD))
      } else {
        this.keyGenerator.getKey(serialize(row)).getRecordKey
      }
    }

    private def getInsertValue(
        record: HoodieRecord[_])
    : Option[IndexedRecord] = {
      toScalaOption(record.toIndexedRecord(avroSchema, payloadProps)).map(_.getData)
    }

    private def merge(curAvroRecord: GenericRecord, newRecord: HoodieRecord[_]): IndexedRecord = {
      newRecord.getData.asInstanceOf[HoodieRecordPayload[_]].combineAndGetUpdateValue(curAvroRecord, avroSchema, payloadProps).get()
    }

    override def close(): Unit = {}
  }
}
