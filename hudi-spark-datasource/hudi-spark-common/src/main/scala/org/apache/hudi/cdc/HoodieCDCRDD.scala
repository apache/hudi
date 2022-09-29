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

import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodieMergeOnReadFileSplit, HoodieTableSchema, HoodieTableState, HoodieUnsafeRDD, LogFileIterator, RecordMergingFileIterator, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils._
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.HoodieCDCInferCase._
import org.apache.hudi.common.table.cdc.HoodieCDCOperation._
import org.apache.hudi.common.table.cdc.{HoodieCDCFileSplit, HoodieCDCSupplementalLoggingMode, HoodieCDCUtils}
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.{HoodiePayloadConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, SparkSession}
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util.Properties
import java.util.stream.Collectors

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The split that will be processed by spark task.
 */
case class HoodieCDCFileGroupSplit(
    commitToChanges: Array[(HoodieInstant, HoodieCDCFileSplit)]
)

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
    changes: Array[HoodieCDCFileGroupSplit])
  extends RDD[InternalRow](spark.sparkContext, Nil) with HoodieUnsafeRDD {

  @transient private val hadoopConf = spark.sparkContext.hadoopConfiguration

  private val confBroadcast = spark.sparkContext.broadcast(new SerializableWritable(hadoopConf))

  private val cdcSupplementalLoggingMode = HoodieCDCSupplementalLoggingMode.parse(
    metaClient.getTableConfig.cdcSupplementalLoggingMode
  )

  private val props = HoodieFileIndex.getConfigProperties(spark, Map.empty)

  protected val payloadProps: Properties = Option(metaClient.getTableConfig.getPreCombineField)
    .map { preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    }.getOrElse(new Properties())

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

    private lazy val fs = metaClient.getFs.getFileSystem

    private lazy val conf = new Configuration(confBroadcast.value.value)

    private lazy val basePath = metaClient.getBasePathV2

    private lazy val tableConfig = metaClient.getTableConfig

    private lazy val populateMetaFields = tableConfig.populateMetaFields()

    private lazy val keyGenerator = {
      val props = new TypedProperties()
      props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, tableConfig.getKeyGeneratorClassName)
      props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key, tableConfig.getRecordKeyFieldProp)
      props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key, tableConfig.getPartitionFieldProp)
      HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    }

    private lazy val recordKeyField: String = if (populateMetaFields) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = metaClient.getTableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

    private lazy val preCombineFieldOpt: Option[String] = Option(metaClient.getTableConfig.getPreCombineField)

    private lazy val tableState = {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
      HoodieTableState(
        pathToString(basePath),
        split.commitToChanges.map(_._1.getTimestamp).max,
        recordKeyField,
        preCombineFieldOpt,
        usesVirtualKeys = false,
        metaClient.getTableConfig.getPayloadClass,
        metadataConfig
      )
    }

    private lazy val mapper: ObjectMapper = {
      val _mapper = new ObjectMapper
      _mapper.setSerializationInclusion(Include.NON_ABSENT)
      _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      _mapper.registerModule(DefaultScalaModule)
      _mapper
    }

    protected override val avroSchema: Schema = new Schema.Parser().parse(originTableSchema.avroSchemaStr)

    protected override val structTypeSchema: StructType = originTableSchema.structTypeSchema

    private lazy val serializer = sparkAdapter.createAvroSerializer(originTableSchema.structTypeSchema,
      avroSchema, nullable = false)

    private lazy val reusableRecordBuilder: GenericRecordBuilder = new GenericRecordBuilder(avroSchema)

    private lazy val cdcAvroSchema: Schema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
      cdcSupplementalLoggingMode,
      HoodieAvroUtils.removeMetadataFields(avroSchema)
    )

    private lazy val cdcSparkSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(cdcAvroSchema)

    /**
     * The deserializer used to convert the CDC GenericRecord to Spark InternalRow.
     */
    private lazy val cdcRecordDeserializer: HoodieAvroDeserializer = {
      sparkAdapter.createAvroDeserializer(cdcAvroSchema, cdcSparkSchema)
    }

    private lazy val projection: UnsafeProjection = generateUnsafeProjection(cdcSchema, requiredCdcSchema)

    // Iterator on cdc file
    private val cdcFileIter = split.commitToChanges.sortBy(_._1).iterator

    // The instant that is currently being processed
    private var currentInstant: HoodieInstant = _

    // The change file that is currently being processed
    private var currentChangeFile: HoodieCDCFileSplit = _

    /**
     * Two cases will use this to iterator the records:
     * 1) extract the change data from the base file directly, including 'ADD_BASE_File' and 'REMOVE_BASE_File'.
     * 2) when the type of cdc file is 'REPLACED_FILE_GROUP',
     *    use this to trace the records that are converted from the '[[beforeImageRecords]]
     */
    private var recordIter: Iterator[InternalRow] = Iterator.empty

    /**
     * Only one case where it will be used is that extract the change data from log files for mor table.
     * At the time, 'logRecordIter' will work with [[beforeImageRecords]] that keep all the records of the previous file slice.
     */
    private var logRecordIter: Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])] = Iterator.empty

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
     * the cdc infer case is [[AS_IS]] and [[cdcSupplementalLoggingMode]] is 'op_key' or 'cdc_data_before'.
     */
    private var afterImageRecords: mutable.Map[String, InternalRow] = mutable.Map.empty

    private def needLoadNextFile: Boolean = {
      !recordIter.hasNext &&
        !logRecordIter.hasNext &&
        (cdcLogRecordIterator == null || !cdcLogRecordIterator.hasNext)
    }

    @tailrec final def hasNextInternal: Boolean = {
      if (needLoadNextFile) {
        loadCdcFile()
      }
      if (currentChangeFile == null) {
        false
      } else {
        currentChangeFile.getCdcInferCase match {
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
      currentChangeFile.getCdcInferCase match {
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
            case HoodieCDCSupplementalLoggingMode.WITH_BEFORE_AFTER =>
              recordToLoad.update(0, convertToUTF8String(String.valueOf(record.get(0))))
              val before = record.get(2).asInstanceOf[GenericRecord]
              recordToLoad.update(2, convertToUTF8String(HoodieCDCUtils.recordToJson(before)))
              val after = record.get(3).asInstanceOf[GenericRecord]
              recordToLoad.update(3, convertToUTF8String(HoodieCDCUtils.recordToJson(after)))
            case HoodieCDCSupplementalLoggingMode.WITH_BEFORE =>
              val row = cdcRecordDeserializer.deserialize(record).get.asInstanceOf[InternalRow]
              val op = row.getString(0)
              val recordKey = row.getString(1)
              recordToLoad.update(0, convertToUTF8String(op))
              val before = record.get(2).asInstanceOf[GenericRecord]
              recordToLoad.update(2, convertToUTF8String(HoodieCDCUtils.recordToJson(before)))
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
                  recordToLoad.update(2, convertRowToJsonString(deserialize(beforeImageRecords(recordKey))))
                  recordToLoad.update(3, convertRowToJsonString(afterImageRecords(recordKey)))
                case _ =>
                  recordToLoad.update(2, convertRowToJsonString(deserialize(beforeImageRecords(recordKey))))
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
     * Load the next log record, and judege how to convert it to cdc format.
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
          recordToLoad.update(2, convertRowToJsonString(deserialize(existingRecordOpt.get)))
          recordToLoad.update(3, null)
          loaded = true
        }
      } else {
        val existingRecordOpt = beforeImageRecords.get(key)
        if (existingRecordOpt.isEmpty) {
          // a new record is inserted.
          val insertedRecord = convertIndexedRecordToRow(indexedRecord.get)
          recordToLoad.update(0, CDCRelation.CDC_OPERATION_INSERT)
          recordToLoad.update(2, null)
          recordToLoad.update(3, convertRowToJsonString(insertedRecord))
          // insert into beforeImageRecords
          beforeImageRecords(key) = serialize(insertedRecord)
          loaded = true
        } else {
          // a existed record is updated.
          val existingRecord = existingRecordOpt.get
          val merged = merge(existingRecord, logRecord)
          val mergeRow = convertIndexedRecordToRow(merged)
          val existingRow = deserialize(existingRecord)
          if (mergeRow != existingRow) {
            recordToLoad.update(0, CDCRelation.CDC_OPERATION_UPDATE)
            recordToLoad.update(2, convertRowToJsonString(existingRow))
            recordToLoad.update(3, convertRowToJsonString(mergeRow))
            // update into beforeImageRecords
            beforeImageRecords(key) = serialize(mergeRow)
            loaded = true
          }
        }
      }
      loaded
    }

    private def loadCdcFile(): Unit = {
      // reset all the iterator or reader first.
      recordIter = Iterator.empty
      logRecordIter = Iterator.empty
      beforeImageRecords.clear()
      afterImageRecords.clear()
      if (cdcLogRecordIterator != null) {
        cdcLogRecordIterator.close()
        cdcLogRecordIterator = null
      }

      if (cdcFileIter.hasNext) {
        val pair = cdcFileIter.next()
        currentInstant = pair._1
        currentChangeFile = pair._2
        currentChangeFile.getCdcInferCase match {
          case BASE_FILE_INSERT =>
            assert(currentChangeFile.getCdcFile != null)
            val absCDCPath = new Path(basePath, currentChangeFile.getCdcFile)
            val fileStatus = fs.getFileStatus(absCDCPath)
            val pf = PartitionedFile(InternalRow.empty, absCDCPath.toUri.toString, 0, fileStatus.getLen)
            recordIter = parquetReader(pf)
          case BASE_FILE_DELETE =>
            assert(currentChangeFile.getBeforeFileSlice.isPresent)
            recordIter = loadFileSlice(currentChangeFile.getBeforeFileSlice.get)
          case LOG_FILE =>
            assert(currentChangeFile.getCdcFile != null && currentChangeFile.getBeforeFileSlice.isPresent)
            loadBeforeFileSliceIfNeeded(currentChangeFile.getBeforeFileSlice.get)
            val absLogPath = new Path(basePath, currentChangeFile.getCdcFile)
            val morSplit = HoodieMergeOnReadFileSplit(None, List(new HoodieLogFile(fs.getFileStatus(absLogPath))))
            val logFileIterator = new LogFileIterator(morSplit, originTableSchema, originTableSchema, tableState, conf)
            logRecordIter = logFileIterator.logRecordsIterator()
          case AS_IS =>
            assert(currentChangeFile.getCdcFile != null)
            // load beforeFileSlice to beforeImageRecords
            if (currentChangeFile.getBeforeFileSlice.isPresent) {
              loadBeforeFileSliceIfNeeded(currentChangeFile.getBeforeFileSlice.get)
            }
            // load afterFileSlice to afterImageRecords
            if (currentChangeFile.getAfterFileSlice.isPresent) {
              val iter = loadFileSlice(currentChangeFile.getAfterFileSlice.get())
              afterImageRecords = mutable.Map.empty
              iter.foreach { row =>
                val key = getRecordKey(row)
                afterImageRecords.put(key, row.copy())
              }
            }
            val absCDCPath = new Path(basePath, currentChangeFile.getCdcFile)
            cdcLogRecordIterator = new HoodieCDCLogRecordIterator(fs, absCDCPath, cdcAvroSchema)
          case REPLACE_COMMIT =>
            if (currentChangeFile.getBeforeFileSlice.isPresent) {
              loadBeforeFileSliceIfNeeded(currentChangeFile.getBeforeFileSlice.get)
            }
            recordIter = beforeImageRecords.values.map { record =>
              deserialize(record)
            }.iterator
            beforeImageRecords.clear()
        }
        resetRecordFormat()
      } else {
        currentInstant = null
        currentChangeFile = null
      }
    }

    /**
     * Initialize the partial fields of the data to be returned in advance to speed up.
     */
    private def resetRecordFormat(): Unit = {
      recordToLoad = currentChangeFile.getCdcInferCase match {
        case BASE_FILE_INSERT =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_INSERT, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case BASE_FILE_DELETE =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case LOG_FILE =>
          InternalRow.fromSeq(Array(
            null, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case AS_IS =>
          InternalRow.fromSeq(Array(
            null, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case REPLACE_COMMIT =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant.getTimestamp),
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
          .map(f => pathToString(f.getPath)).toList
      val same = files.sorted == beforeImageFiles.sorted.toList
      if (!same) {
        // clear up the beforeImageRecords
        beforeImageRecords.clear()
        val iter = loadFileSlice(fileSlice)
        iter.foreach { row =>
          val key = getRecordKey(row)
          beforeImageRecords.put(key, serialize(row))
        }
        // reset beforeImageFiles
        beforeImageFiles.clear()
        beforeImageFiles.append(files: _*)
      }
    }

    private def loadFileSlice(fileSlice: FileSlice): Iterator[InternalRow] = {
      val baseFileStatus = fs.getFileStatus(new Path(fileSlice.getBaseFile.get().getPath))
      val basePartitionedFile = PartitionedFile(
        InternalRow.empty,
        pathToString(baseFileStatus.getPath),
        0,
        baseFileStatus.getLen
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
      val map = scala.collection.mutable.Map.empty[String, Any]
      originTableSchema.structTypeSchema.zipWithIndex.foreach {
        case (field, idx) =>
          if (field.dataType.isInstanceOf[StringType]) {
            map(field.name) = record.getString(idx)
          } else {
            map(field.name) = record.get(idx, field.dataType)
          }
      }
      convertToUTF8String(mapper.writeValueAsString(map))
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

    private def serialize(curRowRecord: InternalRow): GenericRecord = {
      serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]
    }

    private def getRecordKey(row: InternalRow): String = {
      if (populateMetaFields) {
        row.getString(structTypeSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD))
      } else {
        this.keyGenerator.getKey(serialize(row)).getRecordKey
      }
    }

    private def getInsertValue(
        record: HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])
    : Option[IndexedRecord] = {
      toScalaOption(record.getData.getInsertValue(avroSchema, payloadProps))
    }

    private def convertIndexedRecordToRow(record: IndexedRecord): InternalRow = {
      deserialize(
        LogFileIterator.projectAvroUnsafe(record.asInstanceOf[GenericRecord],
          avroSchema, reusableRecordBuilder)
      )
    }

    private def merge(curAvroRecord: GenericRecord, newRecord: HoodieRecord[_ <: HoodieRecordPayload[_]]): IndexedRecord = {
      newRecord.getData.combineAndGetUpdateValue(curAvroRecord, avroSchema, payloadProps).get()
    }

    private def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection =
      HoodieCatalystExpressionUtils.generateUnsafeProjection(from, to)

    override def close(): Unit = {}
  }
}
