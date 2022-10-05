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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import org.apache.hudi.HoodieBaseRelation.{BaseFileReader, projectReader}
import org.apache.hudi.LogFileIterator.CONFIG_INSTANTIATION_LOCK
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import java.io.Closeable

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
 *   This could occur, when row could be merged with corresponding delta-log record leveraging while only having
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
 * @param sc spark's context
 * @param config hadoop configuration
 * @param fileReaders suite of base file readers
 * @param tableSchema table's full schema
 * @param requiredSchema expected (potentially) projected schema
 * @param tableState table's state
 * @param mergeType type of merge performed
 * @param fileSplits target file-splits this RDD will be iterating over
 */
class HoodieMergeOnReadRDD(@transient sc: SparkContext,
                           @transient config: Configuration,
                           fileReaders: HoodieMergeOnReadBaseFileReaders,
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           tableState: HoodieTableState,
                           mergeType: String,
                           @transient fileSplits: Seq[HoodieMergeOnReadFileSplit])
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val confBroadcast = sc.broadcast(new SerializableWritable(config))

  private val whitelistedPayloadClasses: Set[String] = Seq(
    classOf[OverwriteWithLatestAvroPayload]
  ).map(_.getName).toSet

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeOnReadPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val iter = mergeOnReadPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        val projectedReader = projectReader(fileReaders.requiredSchemaReaderSkipMerging, requiredSchema.structTypeSchema)
        projectedReader(dataFileOnlySplit.dataFile.get)

      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        new LogFileIterator(logFileOnlySplit, tableSchema, requiredSchema, tableState, getConfig)

      case split if mergeType.equals(DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL) =>
        val reader = fileReaders.requiredSchemaReaderSkipMerging
        new SkipMergeIterator(split, reader, tableSchema, requiredSchema, tableState, getConfig)

      case split if mergeType.equals(DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL) =>
        val reader = pickBaseFileReader
        new RecordMergingFileIterator(split, reader, tableSchema, requiredSchema, tableState, getConfig)

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

  private def pickBaseFileReader: BaseFileReader = {
    // NOTE: This is an optimization making sure that even for MOR tables we fetch absolute minimum
    //       of the stored data possible, while still properly executing corresponding relation's semantic
    //       and meet the query's requirements.
    //
    //       Here we assume that iff queried table
    //          a) It does use one of the standard (and whitelisted) Record Payload classes
    //       then we can avoid reading and parsing the records w/ _full_ schema, and instead only
    //       rely on projected one, nevertheless being able to perform merging correctly
    if (whitelistedPayloadClasses.contains(tableState.recordPayloadClassName)) {
      fileReaders.requiredSchemaReader
    } else {
      fileReaders.fullSchemaReader
    }
  }

  override protected def getPartitions: Array[Partition] =
    fileSplits.zipWithIndex.map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray

  private def getConfig: Configuration = {
    val conf = confBroadcast.value.value
    CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }
}
