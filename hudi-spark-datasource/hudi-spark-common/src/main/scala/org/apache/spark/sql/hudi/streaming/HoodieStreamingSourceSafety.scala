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

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.{DataSourceReadOptions, DefaultSource, SparkAdapterSupport}
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.CleanerUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.{Dependency, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Conservative reset and history validation for checkpointed latest-state streaming consumers. */
private[streaming] object HoodieStreamingSourceSafety extends SparkAdapterSupport {
  private class RebuildRequired(message: String) extends HoodieException(message)

  // Spark re-plans the last committed batch during restart without executing it. Defer both
  // validation and relation construction until the scheduler asks for this batch's partitions.
  def defer(sqlContext: SQLContext, schema: StructType)(batch: => DataFrame): DataFrame = {
    val rows = new DeferredBatchRDD(sqlContext, () => batch.queryExecution.logical match {
      case relation: LogicalRDD => relation.rdd
      case plan =>
        // Keep the file-group reader's required filters and projection while executing this
        // individual micro-batch as a bounded scan beneath the outer streaming LogicalRDD.
        val boundedPlan = plan.transform {
          case relation: LogicalRelation => relation.copy(isStreaming = false)
        }
        sparkAdapter.getUnsafeUtils.createDataFrameFrom(sqlContext.sparkSession, boundedPlan).queryExecution.toRdd
    })
    sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession, rows, schema, isStreaming = true)
  }

  private class DeferredBatchRDD(sqlContext: SQLContext,
                                 @transient private val build: () => RDD[InternalRow])
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {
    @transient private lazy val delegate = build()

    override protected def getPartitions: Array[Partition] = delegate.partitions

    override protected def getDependencies: Seq[Dependency[_]] = Seq(new OneToOneDependency(delegate))

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
      firstParent[InternalRow].iterator(split, context)
  }

  def strategy(parameters: Map[String, String]): String = {
    val config = DataSourceReadOptions.STREAMING_READ_RECOVERY_STRATEGY
    val value = parameters.getOrElse(config.key(), config.defaultValue())
    require(Set("FAIL", "READ_UPTO_LATEST_COMMIT", "READ_LATEST").contains(value),
      s"Invalid ${config.key()}=$value; expected FAIL, READ_UPTO_LATEST_COMMIT or READ_LATEST")
    value
  }

  def recover(sqlContext: SQLContext,
              metaClient: HoodieTableMetaClient,
              schema: => StructType,
              parameters: Map[String, String],
              strategy: String,
              start: String,
              end: String,
              completionTime: Boolean,
              resumed: Boolean): Option[DataFrame] = {
    lazy val recoverySchema = schema
    try {
      validate(metaClient, start, end, completionTime, resumed)
      None
    } catch {
      case error: RebuildRequired => strategy match {
        case "FAIL" => throw error
        case "READ_LATEST" =>
          Some(sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession,
            sqlContext.sparkContext.emptyRDD[InternalRow], recoverySchema, isStreaming = true))
        case "READ_UPTO_LATEST_COMMIT" =>
          // Spark owns the batch end/checkpoint. Never read a newer snapshot and checkpoint an older end.
          val timeline = metaClient.reloadActiveTimeline()
          val writes = timeline.getCommitsTimeline.filterCompletedInstants()
            .getInstants.asScala.toSeq
          def offset(i: HoodieInstant): String = if (completionTime) i.getCompletionTime else i.requestedTime()
          val included = writes.filter(i => offset(i) <= end)
          val bound = included.map(_.requestedTime()).reduceOption((a, b) => if (a > b) a else b)
          def isTableService(instant: HoodieInstant): Boolean = {
            val operation = if (instant.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION) {
              timeline.readReplaceCommitMetadata(instant).getOperationType
            } else {
              timeline.readCommitMetadata(instant).getOperationType
            }
            Set(WriteOperationType.COMPACT, WriteOperationType.LOG_COMPACT, WriteOperationType.CLUSTER).contains(operation)
          }
          if (bound.isEmpty || writes.exists(i => offset(i) > end && i.requestedTime() <= bound.get && !isTableService(i))) {
            throw new HoodieException("Cannot reconstruct the recovery snapshot at the checkpointed batch end. "
              + "Rebuild downstream tables and checkpoints together.", error)
          }
          val options = parameters ++ Map(
            DataSourceReadOptions.QUERY_TYPE.key() -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
            DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key() -> bound.get)
          val relation = DefaultSource.createRelation(sqlContext, metaClient, recoverySchema, options)
          Some(relation match {
            case fsRelation: HadoopFsRelation => sparkAdapter.createStreamingDataFrame(sqlContext, fsRelation, recoverySchema)
            case _ => sparkAdapter.internalCreateDataFrame(sqlContext.sparkSession,
              sqlContext.baseRelationToDataFrame(relation).queryExecution.toRdd, recoverySchema, isStreaming = true)
          })
      }
    }
  }

  def validate(metaClient: HoodieTableMetaClient,
               start: String,
               end: String,
               completionTime: Boolean,
               resumed: Boolean): Unit = {
    if (start != HoodieTimeline.INIT_INSTANT_TS) {
      validateRetainedRange(metaClient, start, end, completionTime, resumed)
    }
  }

  private def validateRetainedRange(metaClient: HoodieTableMetaClient,
                                    start: String,
                                    end: String,
                                    completionTime: Boolean,
                                    resumed: Boolean): Unit = {
    val timeline = metaClient.reloadActiveTimeline()
    val completed = timeline.filterCompletedInstants().getInstants.asScala.toSeq
    def offset(instant: HoodieInstant): String = {
      if (completionTime) instant.getCompletionTime else instant.requestedTime()
    }
    def fail(reason: String): Nothing = {
      throw new RebuildRequired(s"Streaming source rebuild required for ${metaClient.getBasePath}: $reason. "
        + "Stop downstream consumers and rebuild affected downstream tables and checkpoints in dependency order. "
        + s"To explicitly accept recovery risks, set ${DataSourceReadOptions.STREAMING_READ_RECOVERY_STRATEGY.key()} "
        + "to READ_UPTO_LATEST_COMMIT (snapshot replay) or READ_LATEST (skip the gap).")
    }

    val checkpointInstant = completed.find(i => offset(i) == start)
    if (resumed && checkpointInstant.isEmpty) {
      fail(s"saved offset $start is absent from the active timeline; history expired or the source table was recreated")
    }
    if (end < start) {
      fail(s"source offset moved backwards from $start to $end")
    }

    val firstWrite = timeline.getWriteTimeline.filterCompletedInstants().firstInstant()
    val requestedStart = checkpointInstant.map(_.requestedTime())
    val unreadWrites = timeline.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
      .filter(i => offset(i) > start && offset(i) <= end).toSeq
    val earliestUnread = unreadWrites.map(_.requestedTime()).reduceOption((a, b) => if (a < b) a else b)
    val completedByRequestedTime = completed.map(i => i.requestedTime() -> i).toMap
    // Usually only new cleans matter. Retain earlier cleans for late-completing writes,
    // or a user-specified initial offset whose requested-time boundary is unknown.
    timeline.getCleanerTimeline.filterCompletedInstants().getInstants.asScala
      .filter(clean => offset(clean) > start || requestedStart.isEmpty ||
        requestedStart.exists(_ < clean.requestedTime()) || earliestUnread.exists(_ < clean.requestedTime()))
      .foreach { clean =>
        val metadata = CleanerUtils.getCleanerMetadata(metaClient, clean)
        val retained = metadata.getEarliestCommitToRetain
        // File-version cleaning does not expose a retention boundary. Refuse to infer safety.
        if ((retained == null || retained.isEmpty) && metadata.getTotalFilesDeleted > 0 && offset(clean) > start) {
          fail("the cleaner did not record an earliest retained commit; KEEP_LATEST_FILE_VERSIONS "
            + "cannot prove incremental safety after deleting files")
        }
        val boundary = Option(retained).filter(_.nonEmpty)
        val retainedInstant = completedByRequestedTime.get(retained)
        val expired = boundary.exists { floor =>
          earliestUnread.exists(_ < floor) ||
            (requestedStart.isEmpty && retainedInstant.exists(i => start < offset(i))) ||
            (requestedStart.isEmpty && retainedInstant.isEmpty && firstWrite.isPresent && start < offset(firstWrite.get))
        }
        if (expired) {
          fail(s"history after offset $start has been cleaned (earliest retained commit $retained)")
        }
      }

    completed.filter(i => i.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION &&
      offset(i) > start).foreach { instant =>
      val operation = timeline.readReplaceCommitMetadata(instant).getOperationType
      if (operation != WriteOperationType.CLUSTER) {
        fail(s"logical reset $operation at ${instant.requestedTime()} follows offset $start")
      }
    }
  }
}
