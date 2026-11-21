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

import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.engine.ReaderContextFactory
import org.apache.hudi.common.model.{HoodieFileGroupId, HoodieKey, HoodieRecord, HoodieRecordLocation, HoodieSparkRecord, WriteOperationType}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieClusteringUpdateException
import org.apache.hudi.io.{AppendHandleFactory, HoodieMergeHandleFactory, HoodieWriteMergeHandle, MergeContext, MergeUtils}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.commit.BulkInsertDataInternalWriterHelper

import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Executor-side write task of the DataFrame write path: consumes one Spark partition of routed
 * rows (sorted by the routing-bucket column) and drives one write handle per bucket run.
 *
 * <p>Insert buckets go through {@link BulkInsertDataInternalWriterHelper}, which rotates
 * {@code HoodieRowCreateHandle}s on partition-path change and file-size limits. Update buckets
 * feed a streaming iterator of engine-native records into the merge handle resolved by
 * {@link HoodieMergeHandleFactory} ({@code FileGroupReaderBasedMergeHandle} by default).
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
                         pendingClusteringFileGroups: Set[HoodieFileGroupId]) extends Serializable {

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
    val dataProjection = DataFrameWriteTask.conformingProjection(routedSchema, metaFieldCount, dataFieldCount, writerSchema)
    val dataSchema = writerSchema

    val taskContextSupplier = table.getTaskContextSupplier
    val statuses = new ListBuffer[WriteStatus]()
    val buffered = iter.buffered
    var insertHelper: BulkInsertDataInternalWriterHelper = null

    while (buffered.hasNext) {
      val bucket = buffered.head.getString(bucketOrdinal)
      if (bucket.startsWith(HoodieDataFrameWriter.MERGE_BUCKET_PREFIX)) {
        statuses ++= mergeBucket(buffered, bucket, dataProjection, dataSchema, taskContextSupplier)
      } else if (bucket.startsWith(HoodieDataFrameWriter.UPDATE_BUCKET_PREFIX)) {
        if (mergeOnRead) {
          statuses ++= appendBucket(buffered, bucket, dataProjection, dataSchema, taskContextSupplier)
        } else {
          statuses ++= mergeBucket(buffered, bucket, dataProjection, dataSchema, taskContextSupplier)
        }
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

  /**
   * Merges one contiguous run of update rows (all tagged to the same file group) into that file
   * group by streaming engine-native records into the merge handle.
   */
  private def bucketRecordIterator(buffered: BufferedIterator[InternalRow],
                                   bucket: String,
                                   dataProjection: InternalRow => UnsafeRow,
                                   dataSchema: StructType,
                                   location: Option[HoodieRecordLocation]): java.util.Iterator[HoodieRecord[InternalRow]] = {
    new java.util.Iterator[HoodieRecord[InternalRow]] {
      override def hasNext: Boolean =
        buffered.hasNext && buffered.head.getString(bucketOrdinal) == bucket

      override def next(): HoodieRecord[InternalRow] = {
        val row = buffered.next()
        val key = new HoodieKey(
          row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD),
          row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD))
        val isDelete = row.getString(rowOpOrdinal) == HoodieDataFrameWriter.ROW_OP_DELETE
        // The projection reuses its output buffer, and the merge machinery buffers incoming
        // records, so each record needs its own copy.
        val dataRow = dataProjection(row).copy()
        val record = if (isDelete) {
          new HoodieSparkRecord(key, dataRow, dataSchema, false,
            null.asInstanceOf[org.apache.hudi.common.model.HoodieOperation],
            null.asInstanceOf[Comparable[_]], true)
        } else {
          new HoodieSparkRecord(key, dataRow, dataSchema, false)
        }
        // Append handles book records with a current location as updates in the delta stats.
        location.foreach(record.setCurrentLocation)
        record.asInstanceOf[HoodieRecord[InternalRow]]
      }
    }
  }

  private def mergeBucket(buffered: BufferedIterator[InternalRow],
                          bucket: String,
                          dataProjection: InternalRow => UnsafeRow,
                          dataSchema: StructType,
                          taskContextSupplier: org.apache.hudi.common.engine.TaskContextSupplier): Seq[WriteStatus] = {
    val fileId = bucket.substring(2)
    val partitionPath = buffered.head.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
    rejectUpdateToPendingClustering(partitionPath, fileId)
    val recordItr = bucketRecordIterator(buffered, bucket, dataProjection, dataSchema, None)

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
                           dataProjection: InternalRow => UnsafeRow,
                           dataSchema: StructType,
                           taskContextSupplier: org.apache.hudi.common.engine.TaskContextSupplier): Seq[WriteStatus] = {
    val fileId = bucket.substring(2)
    val partitionPath = buffered.head.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)
    rejectUpdateToPendingClustering(partitionPath, fileId)
    val recordItr = bucketRecordIterator(buffered, bucket, dataProjection, dataSchema,
      Some(new HoodieRecordLocation(instantTime, fileId)))
    val appendHandle = new AppendHandleFactory[InternalRow, AnyRef, AnyRef, AnyRef]().create(
      writeConfig, instantTime,
      table.asInstanceOf[HoodieTable[InternalRow, AnyRef, AnyRef, AnyRef]],
      partitionPath, fileId, recordItr, taskContextSupplier)
    appendHandle.doAppend()
    appendHandle.close().asScala.toSeq
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
