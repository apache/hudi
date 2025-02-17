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

import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieBootstrapMORRDD.{getPartitionPath, CONFIG_INSTANTIATION_LOCK}
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

class HoodieBootstrapMORRDD(@transient spark: SparkSession,
                            @transient config: Configuration,
                            bootstrapDataFileReader: BaseFileReader,
                            bootstrapSkeletonFileReader: BaseFileReader,
                            regularFileReader: BaseFileReader,
                            tableSchema: HoodieTableSchema,
                            requiredSchema: HoodieTableSchema,
                            tableState: HoodieTableState,
                            @transient splits: Seq[BaseHoodieBootstrapSplit])
  extends HoodieBootstrapRDD(spark, bootstrapDataFileReader, bootstrapSkeletonFileReader,
    regularFileReader, requiredSchema, splits) {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val hadoopConfBroadcast = spark.sparkContext.broadcast(new SerializableWritable(config))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[HoodieBootstrapPartition]
    maybeLog(bootstrapPartition)
    val bootstrapMORSplit = bootstrapPartition.split.asInstanceOf[HoodieBootstrapMORSplit]

    if (bootstrapMORSplit.logFiles.isEmpty) {
      //no log files, treat like regular bootstrap
      getIterator(bootstrapPartition)
    } else {
      bootstrapMORSplit.skeletonFile match {
        case Some(skeletonFile) =>
          val (iterator, schema) = getSkeletonIteratorSchema(bootstrapMORSplit.dataFile, skeletonFile)
          new RecordMergingFileIterator(bootstrapMORSplit.logFiles, getPartitionPath(skeletonFile),
            iterator, schema, tableSchema, requiredSchema, tableState, getHadoopConf)
        case _ =>
          // NOTE: Regular file-reader is already projected into the required schema
          new RecordMergingFileIterator(bootstrapMORSplit.logFiles,
            getPartitionPath(bootstrapMORSplit.dataFile),
            regularFileReader.read(bootstrapMORSplit.dataFile), regularFileReader.schema, tableSchema,
            requiredSchema, tableState, getHadoopConf)
      }
    }
  }

  private def getHadoopConf: Configuration = {
    val conf = hadoopConfBroadcast.value.value
    // TODO clean up, this lock is unnecessary see HoodieMergeOnReadRDD
    CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }
}

object HoodieBootstrapMORRDD extends SparkAdapterSupport {
  val CONFIG_INSTANTIATION_LOCK = new Object()

  def getPartitionPath(file: PartitionedFile): StoragePath = {
    sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file).getParent
  }
}
