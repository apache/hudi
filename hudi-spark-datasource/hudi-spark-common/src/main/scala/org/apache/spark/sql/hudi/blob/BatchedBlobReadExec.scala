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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/**
 * Physical plan node that executes batched blob reads.
 *
 * Reads blob data from storage using [[BatchedByteRangeReader]] to batch
 * reads efficiently when data is sorted by file and position.
 *
 * @param child Child physical plan
 * @param maxGapBytes Maximum gap between reads to batch (from config)
 * @param storageConf Storage configuration for file access
 * @param lookaheadSize Number of rows to buffer for batch detection
 * @param logical The logical plan node this was created from
 */
case class BatchedBlobReadExec(child: SparkPlan,
                                maxGapBytes: Int,
                                storageConf: StorageConfiguration[_],
                                lookaheadSize: Int,
                                logical: BatchedBlobRead)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = logical.output

  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()

    // Use direct RDD processing - no DataFrame conversion!
    BatchedByteRangeReader.processRDD(
      childRDD,
      child.schema,
      storageConf,
      maxGapBytes,
      lookaheadSize,
      logical.blobAttr.name
    )
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
