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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.storage.StorageConfiguration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/**
 * Physical plan node that executes batched blob reads.
 *
 * Reads blob data from storage using [[BatchedBlobReader]] to batch
 * reads efficiently when data is sorted by file and position.
 *
 * Holds pre-extracted `blobAttrName` and `output` rather than the originating
 * [[BatchedBlobRead]]: the logical tree reaches `HoodieFileIndex`, which
 * isn't Serializable and would fail task dispatch on executor send.
 *
 * @param child Child physical plan
 * @param maxGapBytes Maximum gap between reads to batch (from config)
 * @param storageConf Storage configuration for file access
 * @param lookaheadSize Number of rows to buffer for batch detection
 * @param blobAttrName Name of the blob column resolved from the logical plan
 * @param output Output attributes resolved from the logical plan
 */
case class BatchedBlobReadExec(child: SparkPlan,
                                maxGapBytes: Int,
                                storageConf: StorageConfiguration[_],
                                lookaheadSize: Int,
                                blobAttrName: String,
                                override val output: Seq[Attribute])
  extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    // Broadcast storageConf to avoid per-task serialization
    val broadcastConf: Broadcast[StorageConfiguration[_]] = childRDD.sparkContext.broadcast(storageConf)
    // Use direct RDD processing - no DataFrame conversion!
    BatchedBlobReader.processRDD(
      childRDD,
      child.schema,
      broadcastConf,
      maxGapBytes,
      lookaheadSize,
      blobAttrName
    )
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
