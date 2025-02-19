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

package org.apache.hudi

import org.apache.hudi.HoodieBaseRelation.{projectReader, BaseFileReader}
import org.apache.hudi.HoodieMergeOnReadRDDV1.CONFIG_INSTANTIATION_LOCK
import org.apache.hudi.MergeOnReadSnapshotRelation.isProjectionCompatible
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

import java.io.Closeable
import java.util.function.Predicate

/**
 * RDD enabling Hudi's Merge-on-Read (MOR) semantic
 *
 * @param sc               spark's context
 * @param config           hadoop configuration
 * @param fileReaders      suite of base file readers
 * @param tableSchema      table's full schema
 * @param requiredSchema   expected (potentially) projected schema
 * @param tableState       table's state
 * @param mergeType        type of merge performed
 * @param fileSplits       target file-splits this RDD will be iterating over
 * @param includeStartTime whether to include the commit with the commitTime
 * @param startTimestamp   start timestamp to filter records
 * @param endTimestamp     end timestamp to filter records
 */
class HoodieMergeOnReadRDDV1(@transient sc: SparkContext,
                             @transient config: Configuration,
                             fileReaders: HoodieMergeOnReadBaseFileReaders,
                             tableSchema: HoodieTableSchema,
                             requiredSchema: HoodieTableSchema,
                             tableState: HoodieTableState,
                             mergeType: String,
                             @transient fileSplits: Seq[HoodieMergeOnReadFileSplit],
                             includeStartTime: Boolean = false,
                             startTimestamp: String = null,
                             endTimestamp: String = null)
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val hadoopConfBroadcast = sc.broadcast(new SerializableWritable(config))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val iter = partition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        val projectedReader = projectReader(fileReaders.requiredSchemaReaderSkipMerging, requiredSchema.structTypeSchema)
        projectedReader(dataFileOnlySplit.dataFile.get)

      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        new LogFileIterator(logFileOnlySplit, tableSchema, requiredSchema, tableState, getHadoopConf)

      case split =>
        mergeType match {
          case DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL =>
            val reader = fileReaders.requiredSchemaReaderSkipMerging
            new SkipMergeIterator(split, reader, tableSchema, requiredSchema, tableState, getHadoopConf)

          case DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
            val reader = pickBaseFileReader()
            new RecordMergingFileIterator(split, reader, tableSchema, requiredSchema, tableState, getHadoopConf)

          case _ => throw new UnsupportedOperationException(s"Not supported merge type ($mergeType)")
        }

      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${partition.split.dataFile.get.filePath}" +
        s"log paths: ${partition.split.logFiles.toString}" +
        s"hoodie table path: ${tableState.tablePath}" +
        s"spark partition Index: ${partition.index}" +
        s"merge type: ${mergeType}")
    }

    if (iter.isInstanceOf[Closeable]) {
      // register a callback to close logScanner which will be executed on task completion.
      // when tasks finished, this method will be called, and release resources.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.asInstanceOf[Closeable].close()))
    }

    val commitTimeMetadataFieldIdx = requiredSchema.structTypeSchema.fieldNames.indexOf(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val needsFiltering = commitTimeMetadataFieldIdx >= 0 && !StringUtils.isNullOrEmpty(startTimestamp) && !StringUtils.isNullOrEmpty(endTimestamp)
    if (needsFiltering) {
      val filterT: Predicate[InternalRow] = getCommitTimeFilter(includeStartTime, commitTimeMetadataFieldIdx)
      iter.filter(filterT.test)
    }
    else {
      iter
    }
  }

  private def getCommitTimeFilter(includeStartTime: Boolean, commitTimeMetadataFieldIdx: Int): Predicate[InternalRow] = {
    if (includeStartTime) {
      new Predicate[InternalRow] {
        override def test(row: InternalRow): Boolean = {
          val commitTime = row.getString(commitTimeMetadataFieldIdx)
          commitTime >= startTimestamp && commitTime <= endTimestamp
        }
      }
    } else {
      new Predicate[InternalRow] {
        override def test(row: InternalRow): Boolean = {
          val commitTime = row.getString(commitTimeMetadataFieldIdx)
          commitTime > startTimestamp && commitTime <= endTimestamp
        }
      }
    }
  }

  private def pickBaseFileReader(): BaseFileReader = {
    // NOTE: This is an optimization making sure that even for MOR tables we fetch absolute minimum
    //       of the stored data possible, while still properly executing corresponding relation's semantic
    //       and meet the query's requirements.
    //
    //       Here we assume that iff queried table does use one of the standard (and whitelisted)
    //       Record Payload classes then we can avoid reading and parsing the records w/ _full_ schema,
    //       and instead only rely on projected one, nevertheless being able to perform merging correctly
    if (isProjectionCompatible(tableState)) {
      fileReaders.requiredSchemaReader
    } else {
      fileReaders.fullSchemaReader
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
}

object HoodieMergeOnReadRDDV1 {
  val CONFIG_INSTANTIATION_LOCK = new Object()
}
