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
 * Reads blob data from storage using [[BatchedBlobReader]] to batch
 * reads efficiently when data is sorted by file and position.
 *
 * Holds pre-extracted `blobAttr` and `output` rather than the originating
 * [[BatchedBlobRead]]: the logical tree reaches `HoodieFileIndex`, which
 * isn't Serializable and would fail task dispatch on executor send. Only plain
 * values (the blob column's ordinal, the base path string) reach executors.
 *
 * @param child Child physical plan
 * @param maxGapBytes Maximum gap between reads to batch (from config)
 * @param storageConf Storage configuration for file access
 * @param lookaheadSize Number of rows to buffer for batch detection
 * @param blobAttr Blob column attribute resolved from the logical plan; located in the child's
 *                 output by exprId, so a same-named column (e.g. from the other side of a join) is never read
 * @param tableBasePath Base path of the Hudi table the blob column comes from (see [[BlobTableBasePath]]),
 *                      used to resolve managed references stored relative to it; None if unknown
 * @param output Output attributes resolved from the logical plan
 * @param tableLayoutVersion Explicit layout version of the producing tables; None for legacy resolution
 */
case class BatchedBlobReadExec(child: SparkPlan,
                                maxGapBytes: Int,
                                storageConf: StorageConfiguration[_],
                                lookaheadSize: Int,
                                blobAttr: Attribute,
                                tableBasePath: Option[String],
                                override val output: Seq[Attribute],
                                tableLayoutVersion: Option[String] = None)
  extends UnaryExecNode {

  /** Constructs the node by its earlier column-name API, without a known table path or layout. */
  def this(child: SparkPlan,
           maxGapBytes: Int,
           storageConf: StorageConfiguration[_],
           lookaheadSize: Int,
           blobAttrName: String,
           output: Seq[Attribute]) =
    this(child, maxGapBytes, storageConf, lookaheadSize, BatchedBlobReadExec.attributeNamed(child, blobAttrName), None, output, None)

  override protected def doExecute(): RDD[InternalRow] = {
    val ordinal = child.output.indexWhere(_.exprId == blobAttr.exprId)
    require(ordinal >= 0, s"Blob column $blobAttr is not in the child output ${child.output.mkString("[", ", ", "]")}")
    val childRDD = child.execute()
    // Broadcast storageConf to avoid per-task serialization
    val broadcastConf = BatchedBlobReader.broadcastStorageConf(childRDD.sparkContext, storageConf)
    // Use direct RDD processing - no DataFrame conversion!
    BatchedBlobReader.processRDDAtOrdinal(
      childRDD,
      child.schema,
      broadcastConf,
      maxGapBytes,
      lookaheadSize,
      ordinal,
      tableBasePath = tableBasePath,
      tableLayoutVersion = tableLayoutVersion
    )
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

object BatchedBlobReadExec {
  /** The earlier factory resolves exactly one output attribute of the requested name. */
  def apply(child: SparkPlan,
            maxGapBytes: Int,
            storageConf: StorageConfiguration[_],
            lookaheadSize: Int,
            blobAttrName: String,
            output: Seq[Attribute]): BatchedBlobReadExec =
    new BatchedBlobReadExec(child, maxGapBytes, storageConf, lookaheadSize, blobAttrName, output)

  private def attributeNamed(child: SparkPlan, name: String): Attribute = {
    val matches = child.output.filter(_.name == name)
    require(matches.size == 1, s"Blob column $name must name exactly one child output attribute, found ${matches.size}")
    matches.head
  }
}
