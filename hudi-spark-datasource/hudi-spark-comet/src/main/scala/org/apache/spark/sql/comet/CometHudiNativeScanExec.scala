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

package org.apache.spark.sql.comet

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Native Hudi scan operator, executing through hudi-rs inside Comet.
 *
 * The Spark side plans the read as usual (Hudi file index, partition pruning, file slice
 * grouping); this node carries the per-partition file slices as serialized planning data. The
 * native operator opens the table from hoodie.properties under the table base URI and reads each
 * slice (Parquet decode plus log-file merge) off the JVM.
 */
case class CometHudiNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: FileSourceScanExec,
    override val serializedPlanOpt: SerializedPlan,
    tableBaseUri: String,
    scanHashCode: Int,
    commonBytes: Array[Byte],
    perPartitionBytes: Array[Array[Byte]])
    extends CometLeafExec
    with CometScanWithPlanData {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometHudiNativeScan"

  // A fused native block takes its partition count from the leaf scan's output
  // partitioning, so it must reflect the number of per-partition slice groups.
  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(perPartitionBytes.length)

  override def sourceKey: String = HudiScanDataKey.key(tableBaseUri, scanHashCode)

  override def commonData: Array[Byte] = commonBytes

  override def perPartitionData: Array[Array[Byte]] = perPartitionBytes

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    new CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> perPartitionData),
      serializedPlan = serializedPlan,
      defaultNumPartitions = perPartitionData.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty)
  }

  override protected def doCanonicalize(): CometHudiNativeScanExec = {
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      originalPlan = null,
      serializedPlanOpt = SerializedPlan(None))
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, tableBaseUri, perPartitionBytes.length)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHudiNativeScanExec =>
        this.nativeOp == other.nativeOp &&
        this.output == other.output &&
        this.tableBaseUri == other.tableBaseUri &&
        this.scanHashCode == other.scanHashCode &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ => false
    }
  }

  override def hashCode(): Int =
    java.util.Objects.hash(nativeOp, output, tableBaseUri, Integer.valueOf(scanHashCode))
}

/**
 * The key both the scan node and [[HudiPlanDataInjector]] derive, so per-partition planning data
 * lands on the right scan when a query reads the same table more than once.
 */
object HudiScanDataKey {
  def key(tableBaseUri: String, scanHashCode: Int): String = s"${tableBaseUri}_$scanHashCode"
}

/**
 * Splices a Hudi scan's planning data into the serialized native plan: the common block (table
 * URI, options, schema) once, and this partition's file slices. Registered with Comet through
 * the JDK ServiceLoader (META-INF/services), the mechanism Comet provides for out-of-tree scan
 * implementations.
 */
class HudiPlanDataInjector extends PlanDataInjector {

  override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.HUDI_SCAN

  override def canInject(op: Operator): Boolean =
    op.hasHudiScan && op.getHudiScan.getFileSlicesCount == 0 && op.getHudiScan.hasCommon

  override def getKey(op: Operator): Option[String] = {
    val common = op.getHudiScan.getCommon
    Some(HudiScanDataKey.key(common.getTableBaseUri, common.getScanHashCode))
  }

  override def inject(
      op: Operator,
      commonBytes: Array[Byte],
      partitionBytes: Array[Byte]): Operator = {
    val common = OperatorOuterClass.HudiScanCommon.parseFrom(commonBytes)
    val partition = OperatorOuterClass.HudiScan.parseFrom(partitionBytes)
    op.toBuilder
      .setHudiScan(
        OperatorOuterClass.HudiScan
          .newBuilder()
          .setCommon(common)
          .addAllFileSlices(partition.getFileSlicesList))
      .build()
  }
}
