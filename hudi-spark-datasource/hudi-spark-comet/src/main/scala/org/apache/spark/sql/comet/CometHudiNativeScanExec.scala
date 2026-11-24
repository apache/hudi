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

import org.apache.hudi.comet.HudiCometScanProvider

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Native Hudi scan operator, executing through hudi-rs inside Comet.
 *
 * The Spark side plans the read as usual (Hudi file index, partition pruning, file slice
 * grouping); this node carries the per-partition file slices as serialized planning data. The
 * native operator opens the table from hoodie.properties under the table base URI and reads each
 * slice (Parquet decode plus log-file merge) off the JVM.
 *
 * The tasks are planned when this node executes, over the partitions the scan lists then.
 * `runtimeFilters` are the partition filters that hold a subquery (dynamic partition pruning, or
 * a scalar subquery); Spark resolves them first, and only the partitions they keep are planned.
 * `canonicalScan` is the Spark scan's canonical form without them; together with the normalized
 * `runtimeFilters` it decides when two scans are the same.
 */
case class CometHudiNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    @transient override val originalPlan: FileSourceScanExec,
    override val serializedPlanOpt: SerializedPlan,
    tableBaseUri: String,
    @transient canonicalScan: SparkPlan,
    scanHashCode: Int,
    commonBytes: Array[Byte],
    isMor: Boolean,
    runtimeFilters: Seq[Expression])
    extends CometLeafExec
    with CometScanWithPlanData {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = "CometHudiNativeScan"

  // The task count is only known once the scan lists at execution. The native RDD is sized from
  // perPartitionData then, not from this.
  override lazy val outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def sourceKey: String =
    HudiScanDataKey.key(tableBaseUri, scanHashCode, nativeOp.getPlanId)

  override def commonData: Array[Byte] = commonBytes

  override def perPartitionData: Array[Array[Byte]] = prunedPartitionBytes

  override def dynamicPruningFilters: Seq[Expression] = runtimeFilters

  override def withDynamicPruningFilters(filters: Seq[Expression]): SparkPlan =
    copy(runtimeFilters = filters)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.baselineMetrics(sparkContext) ++ Map(
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"))

  // The execution these driver metrics were last posted under, so a scan reached both through
  // its own execution and through a fused parent posts once.
  @transient private var postedExecutionId: String = _

  @transient private lazy val prunedPartitionBytes: Array[Array[Byte]] = {
    if (runtimeFilters.nonEmpty) {
      ensureSubqueriesResolved()
    }
    val partitions = prunedPartitions
    // Planning checked a listing of the same snapshot; this only trips when the listing changed
    // in between, where a wrong answer must not be returned.
    def unsupported(reason: String): Nothing = throw new IllegalStateException(
      s"Hudi scan of $tableBaseUri lists $reason at execution, which the native scan does not " +
        s"read. Set ${HudiCometScanProvider.SCAN_ENABLED_KEY}=false to read it on the JVM.")
    val partitionSchema = originalPlan.relation.partitionSchema
    if (output.exists(attr => partitionSchema.fieldNames.contains(attr.name)) &&
      partitions.exists(p => partitionSchema.indices.exists(p.values.isNullAt))) {
      unsupported("a null partition value")
    }
    val bytes = HudiCometScanProvider
      .planPartitions(session, partitions, tableBaseUri, isMor)
      .getOrElse(unsupported("a file slice"))
    metrics("numPartitions").set(partitions.size.toLong)
    metrics("numFiles").set(partitions.map(_.files.size.toLong).sum)
    bytes
  }

  override def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null && executionId == postedExecutionId) {
      return
    }
    prunedPartitionBytes
    SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
      Seq(metrics("numPartitions"), metrics("numFiles")))
    postedExecutionId = executionId
  }

  /** The listed partitions the resolved runtime filters keep, as Spark's own file scan prunes. */
  private def prunedPartitions: Seq[PartitionDirectory] = {
    val listed = originalPlan.selectedPartitions.toSeq
    if (runtimeFilters.isEmpty) {
      listed
    } else {
      val partitionSchema = originalPlan.relation.partitionSchema
      val predicate = Predicate.create(
        runtimeFilters.reduce(And).transform { case a: AttributeReference =>
          val index = partitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
        },
        Nil)
      listed.filter(p => predicate.eval(p.values))
    }
  }

  override def sparkFallback(newChildren: Seq[SparkPlan]): SparkPlan = {
    // AQE converts the pruning placeholders in runtimeFilters, not in originalPlan, so restore
    // the scan with the live filters.
    val staticFilters =
      originalPlan.partitionFilters.filterNot(_.exists(_.isInstanceOf[PlanExpression[_]]))
    val restored = originalPlan.copy(partitionFilters = staticFilters ++ runtimeFilters)
    originalPlan.logicalLink.foreach(restored.setLogicalLink)
    restored
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sendDriverMetrics()
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    new CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> perPartitionData),
      serializedPlan = serializedPlan,
      planFingerprint = PlanDataInjector.planFingerprint(serializedPlan),
      defaultNumPartitions = perPartitionData.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty)
  }

  // The plan id in nativeOp only keys this node's planning data, so it is left out of equality
  // and the canonical form, which lets Spark reuse an exchange or stage over the same scan.
  override protected def doCanonicalize(): CometHudiNativeScanExec = {
    copy(
      nativeOp = nativeOp.toBuilder.setPlanId(0).build(),
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      originalPlan = null,
      serializedPlanOpt = SerializedPlan(None),
      runtimeFilters = QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(runtimeFilters),
        output))
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, tableBaseUri, runtimeFilters)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometHudiNativeScanExec =>
        this.output == other.output &&
        this.tableBaseUri == other.tableBaseUri &&
        this.canonicalScan == other.canonicalScan &&
        this.runtimeFilters == other.runtimeFilters &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ => false
    }
  }

  override def hashCode(): Int =
    java.util.Objects.hash(
      output,
      tableBaseUri,
      Integer.valueOf(scanHashCode),
      runtimeFilters)
}

/**
 * The key both the scan node and [[HudiPlanDataInjector]] derive, so per-partition planning data
 * lands on the right scan when a query reads the same table more than once. Comet requires the
 * key to be unique per scan node in a native plan, which the scan op's plan id provides.
 */
object HudiScanDataKey {
  def key(tableBaseUri: String, scanHashCode: Int, planId: Int): String =
    PlanDataInjector.withPlanId(s"${tableBaseUri}_$scanHashCode", planId)
}

/**
 * Splices a Hudi scan's planning data into the serialized native plan: the common block (table
 * URI, options, schema) once, and this partition's file slices. Registered with Comet through
 * the JDK ServiceLoader (META-INF/services), the mechanism Comet provides for out-of-tree scan
 * implementations.
 */
class HudiPlanDataInjector extends PlanDataInjector {

  override type Prepared = OperatorOuterClass.HudiScanCommon

  override val opStructCase: Operator.OpStructCase = Operator.OpStructCase.HUDI_SCAN

  override def canInject(op: Operator): Boolean =
    op.hasHudiScan && op.getHudiScan.getFileSlicesCount == 0 && op.getHudiScan.hasCommon

  override def getKey(op: Operator): Option[String] = {
    val common = op.getHudiScan.getCommon
    Some(HudiScanDataKey.key(common.getTableBaseUri, common.getScanHashCode, op.getPlanId))
  }

  override def prepareCommon(commonBytes: Array[Byte]): Prepared =
    OperatorOuterClass.HudiScanCommon.parseFrom(commonBytes)

  override def inject(op: Operator, common: Prepared, partitionBytes: Array[Byte]): Operator = {
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
