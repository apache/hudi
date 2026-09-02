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
import org.apache.hudi.common.model.{HoodieKey, HoodieRecord, HoodieSparkRecord, WriteOperationType}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.{HoodieMergeHandleFactory, HoodieWriteMergeHandle, MergeContext, MergeUtils}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.table.HoodieTable
import org.apache.hudi.table.action.commit.BulkInsertDataInternalWriterHelper

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.types.StructType

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
                         routedSchema: StructType,
                         writeSchemaLength: Int,
                         bucketOrdinal: Int,
                         rowOpOrdinal: Int,
                         updateBucketPrefix: String) extends Serializable {

  def execute(iter: Iterator[InternalRow]): Iterator[WriteStatus] = {
    if (!iter.hasNext) {
      Iterator.empty
    } else {
      executeNonEmpty(iter)
    }
  }

  private def executeNonEmpty(iter: Iterator[InternalRow]): Iterator[WriteStatus] = {
    val writeSchema = StructType(routedSchema.fields.take(writeSchemaLength))
    val dataSchema = StructType(routedSchema.fields.slice(
      HoodieRecord.HOODIE_META_COLUMNS.size(), writeSchemaLength))
    val writeProjection = UnsafeProjection.create(
      (0 until writeSchemaLength).map(i =>
        BoundReference(i, routedSchema.fields(i).dataType, nullable = true)))
    val dataProjection = UnsafeProjection.create(
      (HoodieRecord.HOODIE_META_COLUMNS.size() until writeSchemaLength).map(i =>
        BoundReference(i, routedSchema.fields(i).dataType, nullable = true)))

    val taskContextSupplier = table.getTaskContextSupplier
    val statuses = new ListBuffer[WriteStatus]()
    val buffered = iter.buffered
    var insertHelper: BulkInsertDataInternalWriterHelper = null

    while (buffered.hasNext) {
      val bucket = buffered.head.getString(bucketOrdinal)
      if (bucket.startsWith(updateBucketPrefix)) {
        statuses ++= mergeBucket(buffered, bucket, dataProjection, dataSchema, taskContextSupplier)
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
  private def mergeBucket(buffered: BufferedIterator[InternalRow],
                          bucket: String,
                          dataProjection: UnsafeProjection,
                          dataSchema: StructType,
                          taskContextSupplier: org.apache.hudi.common.engine.TaskContextSupplier): Seq[WriteStatus] = {
    val fileId = bucket.substring(updateBucketPrefix.length)
    val partitionPath = buffered.head.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD)

    val recordItr: java.util.Iterator[HoodieRecord[InternalRow]] =
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
          if (isDelete) {
            new HoodieSparkRecord(key, dataRow, dataSchema, false,
              null.asInstanceOf[org.apache.hudi.common.model.HoodieOperation],
              null.asInstanceOf[Comparable[_]], true)
              .asInstanceOf[HoodieRecord[InternalRow]]
          } else {
            new HoodieSparkRecord(key, dataRow, dataSchema, false)
              .asInstanceOf[HoodieRecord[InternalRow]]
          }
        }
      }

    val mergeHandle = HoodieMergeHandleFactory.create(
      WriteOperationType.UPSERT, writeConfig, instantTime,
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
}
