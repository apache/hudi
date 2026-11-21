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

package org.apache.hudi.dataframe

import org.apache.hudi.{AvroConversionUtils, HoodieCreateRecordUtils}
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.avro.{AvroRecordContext, HoodieAvroUtils}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.engine.{ReaderContextFactory, TaskContextSupplier}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieEmptyRecord, HoodieFileGroupId, HoodieKey, HoodieRecord, HoodieRecordLocation, HoodieRecordMerger, HoodieSparkRecord, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaCache}
import org.apache.hudi.common.table.read.{BufferedRecordMergerFactory, DeleteContext}
import org.apache.hudi.common.util.{HoodieRecordUtils, Option => HOption, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieClusteringUpdateException
import org.apache.hudi.execution.SparkLazyInsertIterable
import org.apache.hudi.io.{AppendHandleFactory, CreateHandleFactory, HoodieMergeHandleFactory, HoodieWriteMergeHandle, MergeContext, MergeUtils}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.commit.{BaseWriteHelper, BulkInsertDataInternalWriterHelper}

import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * How routed rows become Avro records for the write handles, for tables whose merging or log
 * format works on Avro: the row is converted with the input schema, rewritten to the writer
 * schema when they differ, and wrapped with the table's payload class.
 *
 * @param attachOrderingValue whether records carry their ordering value, as they do when the
 *                            write combines records and the table has ordering fields.
 * @param recordBasedInserts whether inserts go through the record-based create handle, so the
 *                           payload decides what an insert writes.
 */
case class AvroRecordSpec(sourceSchema: String,
                          writerSchema: String,
                          payloadClass: String,
                          orderingFields: java.util.List[String],
                          attachOrderingValue: Boolean,
                          requiresOrderingValue: Boolean,
                          consistentLogicalTimestamp: Boolean,
                          mergeProps: TypedProperties,
                          recordBasedInserts: Boolean)

/**
 * Executor-side write task of the DataFrame write path: consumes one Spark partition of routed
 * rows (sorted by the routing-bucket column) and drives one write handle per bucket run.
 *
 * <p>Insert buckets go through {@link BulkInsertDataInternalWriterHelper}, which rotates
 * {@code HoodieRowCreateHandle}s on partition-path change and file-size limits. Update buckets
 * feed a streaming iterator of records into the merge handle resolved by
 * {@link HoodieMergeHandleFactory} ({@code FileGroupReaderBasedMergeHandle} by default), or into
 * the append handle on merge-on-read tables. Records are engine-native rows unless
 * {@code avroRecords} is set.
 *
 * <p>With {@code combineRecords}, rows arrive sorted by record key within a bucket and the
 * records of a key are reduced to one with the table's record merger before they reach a handle,
 * which is how the write combines records whose merging a window over the ordering fields
 * cannot express (payload classes, custom mergers).
 */
class DataFrameWriteTask(writeConfig: HoodieWriteConfig,
                         table: HoodieTable[InternalRow, _, _, _],
                         readerContextFactory: ReaderContextFactory[InternalRow],
                         instantTime: String,
                         operation: WriteOperationType,
                         routedSchema: StructType,
                         dataFieldCount: Int,
                         writerSchema: StructType,
                         bucketOrdinal: Int,
                         rowOpOrdinal: Int,
                         mergeOnRead: Boolean,
                         pendingClusteringFileGroups: Set[HoodieFileGroupId],
                         avroRecords: Option[AvroRecordSpec],
                         combineRecords: Boolean) extends Serializable {

  import DataFrameWriteTask.RecordFactory

  private val recordType: HoodieRecordType =
    if (avroRecords.isDefined) HoodieRecordType.AVRO else HoodieRecordType.SPARK

  @transient private var recordReducer: Option[(HoodieRecord[InternalRow], HoodieRecord[InternalRow]) => HoodieRecord[InternalRow]] = None

  def execute(iter: Iterator[InternalRow]): Iterator[WriteStatus] = {
    if (!iter.hasNext) {
      Iterator.empty
    } else {
      executeNonEmpty(iter)
    }
  }

  private def executeNonEmpty(iter: Iterator[InternalRow]): Iterator[WriteStatus] = {
    val metaFieldCount = HoodieRecord.HOODIE_META_COLUMNS.size()
    val writeSchema = StructType(routedSchema.fields.take(metaFieldCount) ++ writerSchema.fields)
    val writeProjection = DataFrameWriteTask.conformingProjection(routedSchema, 0, metaFieldCount + dataFieldCount, writeSchema)
    val recordFactory = avroRecords.map(avroRecordFactory).getOrElse(sparkRecordFactory)
    val recordBasedInserts = avroRecords.exists(_.recordBasedInserts)
    recordReducer = if (combineRecords) Some(createRecordReducer()) else None

    val taskContextSupplier = table.getTaskContextSupplier
    val statuses = new ListBuffer[WriteStatus]()
    val buffered = iter.buffered
    var insertHelper: BulkInsertDataInternalWriterHelper = null

    while (buffered.hasNext) {
      val bucket = buffered.head.getString(bucketOrdinal)
      if (bucket.startsWith(HoodieDataFrameWriter.MERGE_BUCKET_PREFIX)) {
        statuses ++= mergeBucket(buffered, bucket, recordFactory, taskContextSupplier)
      } else if (bucket.startsWith(HoodieDataFrameWriter.UPDATE_BUCKET_PREFIX)) {
        if (mergeOnRead) {
          statuses ++= appendBucket(buffered, bucket, recordFactory, taskContextSupplier)
        } else {
          statuses ++= mergeBucket(buffered, bucket, recordFactory, taskContextSupplier)
        }
      } else if (recordBasedInserts) {
        statuses ++= insertBucket(buffered, bucket, recordFactory, taskContextSupplier)
      } else {
        if (insertHelper == null) {
          val partitionId = taskContextSupplier.getPartitionIdSupplier.get
          val taskId = taskContextSupplier.getStageIdSupplier.get.toLong
          val taskEpochId = taskContextSupplier.getAttemptIdSupplier.get
          insertHelper = new BulkInsertDataInternalWriterHelper(
            table, writeConfig, instantTime, partitionId, taskId, taskEpochId, writeSchema,
            writeConfig.populateMetaFields(), true)
        }
        insertHelper.write(writeProjection(buffered.next()))
      }
    }
    if (insertHelper != null) {
      statuses ++= insertHelper.getWriteStatuses.asScala
    }
    statuses.iterator
  }

  private def sparkRecordFactory: RecordFactory = {
    val dataProjection = DataFrameWriteTask.conformingProjection(
      routedSchema, HoodieRecord.HOODIE_META_COLUMNS.size(), dataFieldCount, writerSchema)
    (row, key, isDelete) => {
      // The projection reuses its output buffer, and the merge machinery buffers incoming
      // records, so each record needs its own copy.
      val dataRow = dataProjection(row).copy()
      if (isDelete) {
        new HoodieSparkRecord(key, dataRow, writerSchema, false,
          null.asInstanceOf[org.apache.hudi.common.model.HoodieOperation],
          null.asInstanceOf[Comparable[_]], true)
      } else {
        new HoodieSparkRecord(key, dataRow, writerSchema, false)
      }
    }
  }

  private def avroRecordFactory(spec: AvroRecordSpec): RecordFactory = {
    val metaFieldCount = HoodieRecord.HOODIE_META_COLUMNS.size()
    val sourceStructType = StructType(routedSchema.fields.slice(metaFieldCount, metaFieldCount + dataFieldCount))
    val sourceSchema = HoodieSchema.parse(spec.sourceSchema)
    val targetSchema = HoodieSchema.parse(spec.writerSchema)
    val dataProjection = UnsafeProjection.create(
      (metaFieldCount until metaFieldCount + dataFieldCount).map(i =>
        BoundReference(i, routedSchema.fields(i).dataType, nullable = true)))
    val toAvro = AvroConversionUtils.createInternalRowToAvroConverter(sourceStructType, sourceSchema, sourceSchema.isNullable)
    val sameSchema = sourceSchema == targetSchema
    val targetAvroSchema = targetSchema.toAvroSchema
    val deleteContext = new DeleteContext(spec.mergeProps, targetSchema).withReaderSchema(targetSchema)
    (row, key, isDeleteOperation) => {
      val converted = toAvro(dataProjection(row))
      val avroRecord = if (sameSchema) converted else HoodieAvroUtils.rewriteRecordDeep(converted, targetAvroSchema)
      val isDelete = isDeleteOperation ||
        AvroRecordContext.getFieldAccessorInstance.isDeleteRecord(avroRecord, deleteContext)
      val record = if (spec.attachOrderingValue) {
        val orderingValue = HoodieCreateRecordUtils.getOrderingValue(spec.orderingFields, avroRecord,
          key.getRecordKey, spec.consistentLogicalTimestamp, spec.requiresOrderingValue)
        HoodieRecordUtils.createHoodieRecord(avroRecord, orderingValue, key, spec.payloadClass, isDelete)
      } else {
        HoodieRecordUtils.createHoodieRecord(avroRecord, key, spec.payloadClass, isDelete)
      }
      record.asInstanceOf[HoodieRecord[_]]
    }
  }

  /** The records of one contiguous bucket run, combined per key when the write combines records. */
  private def bucketRecordIterator(buffered: BufferedIterator[InternalRow],
                                   bucket: String,
                                   recordFactory: RecordFactory,
                                   location: Option[HoodieRecordLocation]): java.util.Iterator[HoodieRecord[InternalRow]] = {
    val records = bucketRowRecords(buffered, bucket, recordFactory)
    val combined = recordReducer match {
      case None => records
      case Some(combine) =>
        // Rows are sorted by record key within the bucket, so the records of a key are adjacent.
        new Iterator[HoodieRecord[InternalRow]] {
          private val source = records.buffered
          override def hasNext: Boolean = source.hasNext
          override def next(): HoodieRecord[InternalRow] = {
            var record = source.next()
            while (source.hasNext && source.head.getKey == record.getKey) {
              record = combine(record, source.next())
            }
            record
          }
        }
    }
    combined.map { record =>
      // Append handles book records with a current location as updates in the delta stats.
      location.foreach(record.setCurrentLocation)
      record
    }.asJava
  }

  /**
   * The function that reduces two records of a key to one, set up the way the write client sets
   * up record deduplication.
   */
  private def createRecordReducer(): (HoodieRecord[InternalRow], HoodieRecord[InternalRow]) => HoodieRecord[InternalRow] = {
    val readerContext = readerContextFactory.getContext
    readerContext.initRecordMergerForIngestion(writeConfig.getProps)
    val orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(readerContext.getMergeMode, table.getMetaClient)
      .toArray(Array.empty[String])
    val recordSchema = HoodieSchemaCache.intern(HoodieSchema.parse(
      if (StringUtils.nonEmpty(writeConfig.getPartialUpdateSchema)) writeConfig.getPartialUpdateSchema else writeConfig.getWriteSchema))
    val mergeProps = readerContext.getMergeProps(writeConfig.getProps)
    val merger = BufferedRecordMergerFactory.create(
      readerContext,
      readerContext.getMergeMode,
      false,
      readerContext.getRecordMerger.map[HoodieRecordMerger](m => HoodieRecordUtils.mergerToPreCombineMode(m)),
      HOption.ofNullable(writeConfig.getPayloadClass),
      recordSchema,
      mergeProps,
      table.getMetaClient.getTableConfig.getPartialUpdateMode)
    val schema = HoodieSchema.parse(writeConfig.getSchema)
    val recordContext = readerContext.getRecordContext
    val deleteContext = DeleteContext.fromRecordSchema(mergeProps, schema)
    (previous, next) => BaseWriteHelper.reduceRecords(
      mergeProps, merger, orderingFieldNames, previous, next, schema, recordContext, deleteContext)
  }

  /** The records of one contiguous bucket run, read off the shared row iterator. */
  private def bucketRowRecords(rows: BufferedIterator[InternalRow],
                               bucket: String,
                               recordFactory: RecordFactory): Iterator[HoodieRecord[InternalRow]] = {
    new Iterator[HoodieRecord[InternalRow]] {
      override def hasNext: Boolean =
        rows.hasNext && rows.head.getString(bucketOrdinal) == bucket

      override def next(): HoodieRecord[InternalRow] = {
        val row = rows.next()
        val key = new HoodieKey(
          row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD),
          row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD))
        val isDelete = row.getString(rowOpOrdinal) == HoodieDataFrameWriter.ROW_OP_DELETE
        val record: HoodieRecord[_] = if (operation == WriteOperationType.DELETE) {
          // A delete carries no row, only the key of the record to remove.
          new HoodieEmptyRecord(key, recordType)
        } else {
          recordFactory(row, key, isDelete)
        }
        record.asInstanceOf[HoodieRecord[InternalRow]]
      }
    }
  }

  /**
   * Merges one contiguous run of rows (all routed to the same file group) into that file group
   * by streaming records into the merge handle.
   */
  private def mergeBucket(buffered: BufferedIterator[InternalRow],
                          bucket: String,
                          recordFactory: RecordFactory,
                          taskContextSupplier: TaskContextSupplier): Seq[WriteStatus] = {
    val fileId = bucket.substring(2)
    val partitionPath = buffered.head.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
    rejectUpdateToPendingClustering(partitionPath, fileId)
    val recordItr = bucketRecordIterator(buffered, bucket, recordFactory, None)

    val mergeHandle = HoodieMergeHandleFactory.create(
      operation, writeConfig, instantTime,
      table.asInstanceOf[HoodieTable[InternalRow, AnyRef, AnyRef, AnyRef]],
      MergeContext.create(recordItr), partitionPath, fileId, taskContextSupplier,
      HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig))
    mergeHandle match {
      case writeMergeHandle: HoodieWriteMergeHandle[_, _, _, _] =>
        writeMergeHandle.asInstanceOf[HoodieWriteMergeHandle[InternalRow, AnyRef, AnyRef, AnyRef]]
          .setReaderContext(readerContextFactory.getContext)
      case _ =>
    }
    MergeUtils.runMerge(mergeHandle, instantTime, fileId).asScala.flatMap(_.asScala).toSeq
  }

  /** MOR update run: append the bucket's records as a new log file of its file group. */
  private def appendBucket(buffered: BufferedIterator[InternalRow],
                           bucket: String,
                           recordFactory: RecordFactory,
                           taskContextSupplier: TaskContextSupplier): Seq[WriteStatus] = {
    val fileId = bucket.substring(2)
    val partitionPath = buffered.head.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
    rejectUpdateToPendingClustering(partitionPath, fileId)
    val recordItr = bucketRecordIterator(buffered, bucket, recordFactory,
      Some(new HoodieRecordLocation(instantTime, fileId)))
    val appendHandle = new AppendHandleFactory[InternalRow, AnyRef, AnyRef, AnyRef]().create(
      writeConfig, instantTime,
      table.asInstanceOf[HoodieTable[InternalRow, AnyRef, AnyRef, AnyRef]],
      partitionPath, fileId, recordItr, taskContextSupplier)
    appendHandle.doAppend()
    appendHandle.close().asScala.toSeq
  }

  /** Insert run through the record-based create handle, which rolls files over on size. */
  private def insertBucket(buffered: BufferedIterator[InternalRow],
                           bucket: String,
                           recordFactory: RecordFactory,
                           taskContextSupplier: TaskContextSupplier): Seq[WriteStatus] = {
    val recordItr = bucketRecordIterator(buffered, bucket, recordFactory, None)
    val writeStatuses: java.util.Iterator[java.util.List[WriteStatus]] =
      new SparkLazyInsertIterable[InternalRow](recordItr, true, writeConfig, instantTime, table,
        FSUtils.createNewFileIdPfx(), taskContextSupplier,
        new CreateHandleFactory[InternalRow, AnyRef, AnyRef, AnyRef]())
    writeStatuses.asScala.flatMap(_.asScala).toList
  }

  /** Updates to a file group that a pending clustering is rewriting are rejected. */
  private def rejectUpdateToPendingClustering(partitionPath: String, fileId: String): Unit = {
    val fileGroup = new HoodieFileGroupId(partitionPath, fileId)
    if (pendingClusteringFileGroups.contains(fileGroup)) {
      throw new HoodieClusteringUpdateException(
        s"Not allowed to update the clustering file group $fileGroup. "
          + "For pending clustering operations, we are not going to support update for now.")
    }
  }
}

object DataFrameWriteTask {

  /** Builds the record a handle takes from a routed row, its key, and whether it is a delete. */
  private[dataframe] type RecordFactory = (InternalRow, HoodieKey, Boolean) => HoodieRecord[_]

  /**
   * Projects `count` columns of `source` starting at `offset` into rows of `target`. Columns that
   * already line up with the target are copied by position; otherwise the row is rewritten by
   * field name, which reorders columns, fills missing ones with nulls and widens types.
   */
  def conformingProjection(source: StructType, offset: Int, count: Int, target: StructType): InternalRow => UnsafeRow = {
    val slice = StructType(source.fields.slice(offset, offset + count))
    val byPosition = UnsafeProjection.create(
      (offset until offset + count).map(i => BoundReference(i, source.fields(i).dataType, nullable = true)))
    if (sameIgnoringNullability(slice, target)) {
      row => byPosition(row)
    } else {
      row => HoodieInternalRowUtils.getCachedUnsafeRowWriter(slice, target)(byPosition(row))
    }
  }

  private def sameIgnoringNullability(left: DataType, right: DataType): Boolean = (left, right) match {
    case (l: StructType, r: StructType) =>
      l.length == r.length && l.fields.zip(r.fields).forall { case (lf, rf) =>
        lf.name == rf.name && sameIgnoringNullability(lf.dataType, rf.dataType)
      }
    case (l: ArrayType, r: ArrayType) => sameIgnoringNullability(l.elementType, r.elementType)
    case (l: MapType, r: MapType) =>
      sameIgnoringNullability(l.keyType, r.keyType) && sameIgnoringNullability(l.valueType, r.valueType)
    case _ => left == right
  }
}
