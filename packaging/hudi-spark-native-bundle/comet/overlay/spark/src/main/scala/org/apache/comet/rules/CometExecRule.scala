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

package org.apache.comet.rules

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.comet.shims.{ShimCometArrowEvalPythonExec, ShimCometEmptyRelation, ShimCometOneRowRelation}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, BroadcastQueryStageExec, LogicalQueryStage, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2CommandExec}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, Exchange, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometExplainInfo, ConfigEntry, ExtendedExplainInfo}
import org.apache.comet.CometConf.{COMET_SPARK_TO_ARROW_ENABLED, COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST}
import org.apache.comet.CometSparkSessionExtensions._
import org.apache.comet.rules.CometExecRule.allExecs
import org.apache.comet.serde._
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator._
import org.apache.comet.shims.{CometTypeShim, ShimCometMergeRows, ShimCometStreaming, ShimCometWindowGroupLimit, ShimSubqueryBroadcast}

object CometExecRule {

  /**
   * Whether `scan` reads a relation stored in Comet's cache format. Comet's serializer stores
   * that format only for schemas it supports and delegates everything else to Spark's default
   * cache format, which the native scan cannot read.
   */
  private[rules] def readsCometCacheFormat(scan: InMemoryTableScanExec): Boolean =
    scan.relation.cacheBuilder.serializer.isInstanceOf[ArrowCachedBatchSerializer] &&
      ArrowCachedBatchSerializer.supportsSchema(scan.relation.output)

  private[rules] def removePlaceholders(plan: SparkPlan): SparkPlan = plan.transformUp {
    // revertUnsafePartialAggregates re-runs transform over already wrapped query stages, which
    // can produce CometSinkPlaceHolder(CometSinkPlaceHolder(stage)). Remove sinks bottom-up.
    case CometSinkPlaceHolder(_, _, child) => child
    // Scan wrappers are leaves. Recurse explicitly to preserve the old top-down cleanup's
    // coverage of the wrapped plan's descendants as well as remove wrappers at its root.
    case CometScanWrapper(_, wrapped) => removePlaceholders(wrapped)
  }

  /**
   * Tag applied to Partial-mode aggregate operators, and to the PartialMerge stages above them,
   * that must NOT be converted to Comet because a corresponding buffer-consuming aggregate cannot
   * be converted, and the aggregate functions have incompatible intermediate buffer formats
   * between Spark and Comet.
   */
  val COMET_UNSAFE_PARTIAL: TreeNodeTag[String] =
    TreeNodeTag[String]("comet.unsafePartialAgg")

  /**
   * Info message for a native operator that keeps a `Scan` over an input that now reads its
   * shuffle directly, because its native plan's leaves do not line up with its inputs.
   */
  val STALE_SCAN_KEPT: String =
    "Shuffle direct read not applied: the native plan's leaves do not match its inputs"

  /**
   * Fully native operators.
   */
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map[Class[_ <: SparkPlan], CometOperatorSerde[_]](
      classOf[ProjectExec] -> CometProjectExec,
      classOf[FilterExec] -> CometFilterExec,
      classOf[LocalLimitExec] -> CometLocalLimitExec,
      classOf[GlobalLimitExec] -> CometGlobalLimitExec,
      classOf[ExpandExec] -> CometExpandExec,
      classOf[GenerateExec] -> CometExplodeExec,
      classOf[HashAggregateExec] -> CometHashAggregateExec,
      classOf[ObjectHashAggregateExec] -> CometObjectHashAggregateExec,
      classOf[SortAggregateExec] -> CometSortAggregateExec,
      classOf[BroadcastHashJoinExec] -> CometBroadcastHashJoinExec,
      classOf[BroadcastNestedLoopJoinExec] -> CometBroadcastNestedLoopJoinExec,
      classOf[ShuffledHashJoinExec] -> CometHashJoinExec,
      classOf[SortMergeJoinExec] -> CometSortMergeJoinExec,
      classOf[SortExec] -> CometSortExec,
      classOf[LocalTableScanExec] -> CometLocalTableScanExec,
      classOf[RangeExec] -> CometRangeExec,
      classOf[InMemoryTableScanExec] -> CometInMemoryTableScanExec,
      classOf[SampleExec] -> CometSampleExec,
      classOf[WindowExec] -> CometWindowExec) ++
      // EmptyRelationExec was introduced in Spark 4.0.
      ShimCometEmptyRelation.emptyRelationClass.map(_ -> CometEmptyRelationExec) ++
      ShimCometArrowEvalPythonExec.nativeOperator ++
      // WindowGroupLimitExec exists only on Spark 3.5+; the shim returns None on 3.4.
      ShimCometWindowGroupLimit.windowGroupLimitClass.map(_ -> CometWindowGroupLimitExec) ++
      // MergeRowsExec exists only on Spark 3.5+; the shim is empty on 3.4.
      ShimCometMergeRows.nativeExecs

  /**
   * Sinks that have a native plan of ScanExec.
   */
  val sinks: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(
      classOf[CoalesceExec] -> CometCoalesceExec,
      classOf[CollectLimitExec] -> CometCollectLimitExec,
      classOf[TakeOrderedAndProjectExec] -> CometTakeOrderedAndProjectExec,
      classOf[UnionExec] -> CometUnionExec)

  val allExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = nativeExecs ++ sinks

  /**
   * Output path of the write that a `WriteFilesExec` belongs to, copied from the enclosing
   * `InsertIntoHadoopFsRelationCommand`. `WriteFilesExec` itself has no output path, and
   * `CometOperatorSerde` only ever sees the operator, so this is how
   * [[org.apache.comet.serde.operator.CometWriteFiles]] learns the target filesystem. Set
   * immediately before the one `convertToComet` call that reads it, from the command that owns
   * the path; the absence of the tag means the write came from somewhere else and must be
   * declined. Only used on Spark 4.0+; see `CometWriteFilesExec`.
   */
  val WRITE_OUTPUT_PATH: TreeNodeTag[String] = TreeNodeTag[String]("comet.writeOutputPath")

  /**
   * Tag set on a `ShuffleExchangeExec` that should be left as a plain Spark shuffle rather than
   * wrapped in `CometShuffleExchangeExec`. See `tagRedundantColumnarShuffle`.
   */
  val SKIP_COMET_SHUFFLE_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometShuffle")

  /**
   * Tag set on a `BroadcastExchangeExec` that should be left as a plain Spark broadcast rather
   * than converted to `CometBroadcastExchangeExec`. Written by [[CometSpark34AqeDppFallbackRule]]
   * on Spark < 3.5. See that rule's class docstring for the rationale.
   */
  val SKIP_COMET_BROADCAST_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.skipCometBroadcast")

  /**
   * The operators `spark.comet.sparkToColumnar.supportedOperatorList` named by default, before
   * each had a `spark.comet.convert` config of its own. `spark.comet.sparkToColumnar.enabled`
   * still converts them when the list is not set, which is deprecated.
   */
  private val deprecatedSparkToArrowOperators =
    Seq("Range", "InMemoryTableScan", "RDDScan", "OneRowRelation")

  /** Keys of the `spark.comet.convert` configs whose deprecated alternative has been warned. */
  private[rules] val warnedDeprecatedConversions = ConcurrentHashMap.newKeySet[String]()

  /**
   * Tag set on a `SerializeFromObjectExec` whose output an operator above it can stop reading
   * early, naming that operator. See `tagPartiallyReadTypedDatasetOutputs`.
   */
  private val TYPED_DATASET_PARTIAL_READER: TreeNodeTag[String] =
    TreeNodeTag[String]("comet.typedDatasetPartialReader")

  /** Why Comet does not convert rows to Arrow in a plan that reads `InputFileBlockHolder`. */
  private val INPUT_FILE_BLOCK_FALLBACK_REASON: String =
    "Spark to Arrow conversion is not compatible with input_file_name, " +
      "input_file_block_start, or input_file_block_length"

  /**
   * Why Comet does not bridge a Text scan on a broadcast build side. A Text file can hold bytes
   * that are not valid UTF-8, and the conversion replaces them with U+FFFD. See issue #7118.
   */
  private val TEXT_BUILD_SIDE_FALLBACK_REASON: String =
    "Comet converts a Text scan on a broadcast build side to Arrow only when " +
      s"${CometConf.COMET_CONVERT_FROM_TEXT_ENABLED.key}=true, because a Text " +
      "file can hold bytes that are not valid UTF-8"

  /**
   * Tag set on a scan leaf that sits on a broadcast join's build side. When
   * `spark.comet.convert.broadcastBuildSide.enabled` is on, such a scan is bridged to Arrow with
   * `CometSparkToColumnarExec` even when the general sparkToColumnar path is off, so the build
   * branch and the broadcast join can run natively. The build side is usually small, so the row
   * to Arrow copy is usually cheap - see `tagBroadcastBuildSideLeaves` for the caveats. See issue
   * #6008.
   */
  val BROADCAST_BUILD_SIDE_TAG: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit] =
    org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]("comet.broadcastBuildSideLeaf")
}

/**
 * Spark physical optimizer rule for replacing Spark operators with Comet operators.
 *
 * @param queryStagePrep
 *   true when the rule runs as an AQE query stage preparation rule, on the initial plan and on
 *   each re-plan, rather than as the columnar rule that prepares each query stage and the final
 *   plan.
 */
case class CometExecRule(session: SparkSession, queryStagePrep: Boolean = false)
    extends Rule[SparkPlan]
    with CometTypeShim
    with ShimSubqueryBroadcast {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  /**
   * Revert any `CometShuffleExchangeExec` with `CometColumnarShuffle` whose parent and child are
   * both non-Comet aggregate operators (any `BaseAggregateExec`) back to the original Spark
   * `ShuffleExchangeExec`. This is the partial-final-aggregate pattern where Comet couldn't
   * convert either aggregate; keeping a columnar shuffle between them only adds
   * row->arrow->shuffle->arrow->row conversion overhead with no Comet consumer on either side.
   * See https://github.com/apache/datafusion-comet/issues/4004. A native shuffle over rows that
   * `convertShuffleInput` converted is the same pattern, and is reverted the same way. A
   * `SortAggregateExec` with grouping keys reads the shuffle through the `SortExec` that
   * `EnsureRequirements` adds, so it is not the shuffle's parent and the shuffle stays.
   *
   * The match is intentionally narrow (both sides must be row-based aggregates that remained JVM
   * after the main transform pass). Running the revert post-transform means we only fire when the
   * main conversion already decided to keep both aggregates JVM - we never create the dangerous
   * mixed mode where a Comet partial feeds a JVM final (see issue #1389).
   *
   * Correctness depends on running as part of `preColumnarTransitions`: if the revert ran after
   * Spark inserted `ColumnarToRowExec` between the aggregate and the columnar shuffle, the
   * pattern would no longer match (the shuffle would be separated from the aggregate by the
   * transition) and the unnecessary conversion could not be eliminated.
   *
   * The reverted shuffle is tagged with `SKIP_COMET_SHUFFLE_TAG` so both the AQE
   * `QueryStagePrepRule` pass and the `ColumnarRule` `preColumnarTransitions` pass leave it alone
   * on re-entry - AQE in particular re-runs the rule on each stage in isolation, where the outer
   * aggregate context is no longer visible and the shuffle would otherwise be re-wrapped as a
   * Comet columnar shuffle.
   */
  private def revertRedundantColumnarShuffle(plan: SparkPlan): SparkPlan = {
    def isAggregate(p: SparkPlan): Boolean = p.isInstanceOf[BaseAggregateExec]

    // The Spark operator whose rows a Comet shuffle writes: the child of a JVM columnar shuffle,
    // or the input of the conversion that `convertShuffleInput` put under a native shuffle.
    def rowInput(s: CometShuffleExchangeExec): Option[SparkPlan] =
      (s.shuffleType, s.child) match {
        case (CometColumnarShuffle, child) => Some(child)
        case (CometNativeShuffle, conversion: CometSparkToColumnarExec) => Some(conversion.child)
        case _ => None
      }

    def redundantShuffleInput(child: SparkPlan): Option[SparkPlan] = child match {
      case s: CometShuffleExchangeExec => rowInput(s).filter(isAggregate)
      case _ => None
    }

    plan.transform {
      case op if isAggregate(op) && op.children.exists(redundantShuffleInput(_).isDefined) =>
        val newChildren = op.children.map { child =>
          (child, redundantShuffleInput(child)) match {
            case (s: CometShuffleExchangeExec, Some(input)) =>
              val reverted =
                s.originalPlan.withNewChildren(Seq(input)).asInstanceOf[ShuffleExchangeExec]
              reverted.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
              logInfo(
                "Reverting Comet shuffle to Spark shuffle between " +
                  s"${op.getClass.getSimpleName} and ${input.getClass.getSimpleName} " +
                  "(no Comet operator on either side to consume columnar output)")
              reverted
            case _ => child
          }
        }
        op.withNewChildren(newChildren)
    }
  }

  private def shouldSkipCometShuffle(s: ShuffleExchangeExec): Boolean =
    s.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined

  private def applyCometShuffle(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        s
      case s: ShuffleExchangeExec =>
        CometShuffleExchangeExec.shuffleSupported(s) match {
          case Some(CometNativeShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
          case Some(CometColumnarShuffle) =>
            CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
          case None =>
            s
        }
    }
  }

  private def isCometNative(op: SparkPlan): Boolean = op.isInstanceOf[CometNativeExec]

  /**
   * Restore a Spark Partial while retaining its current children. The tag prevents reconversion
   * when AQE replans the exchange without its Final, and records why the Partial stays in Spark.
   */
  private def restoreSparkPartial(agg: CometBaseAggregateExec, reason: String): SparkPlan = {
    val partial = agg.originalPlan.withNewChildren(agg.children)
    partial.setTagValue(CometExecRule.COMET_UNSAFE_PARTIAL, reason)
    withFallbackReason(partial, reason)
  }

  /**
   * A Celeborn exchange can fall back after its child has been converted, for example because of
   * the partition threshold or an unsupported hash key. Keep incompatible partial aggregate
   * buffers on Spark too: an ordinary shuffle cannot connect a native partial to a native final.
   * Run before AQE materializes the producer stage, and retain the tag on later rule passes.
   */
  private def preserveSparkAggregateBuffers(exchange: ShuffleExchangeExec): SparkPlan = {
    if (!isCometCelebornShuffleManagerEnabled(exchange.conf)) return exchange

    val reason = "Partial aggregate disabled: Celeborn exchange falls back to Spark " +
      "and intermediate buffer formats are incompatible"

    def restore(plan: SparkPlan): SparkPlan = plan match {
      // Do not rewrite data that an earlier stage may already have materialized.
      case _: QueryStageExec | _: ShuffleExchangeLike | _: BroadcastExchangeLike => plan
      case agg: CometBaseAggregateExec
          if agg.modes == Seq(Partial) &&
            !QueryPlanSerde.allAggsSupportNativePartialToSparkFinal(agg.aggregateExpressions) =>
        restoreSparkPartial(agg, reason)
      // Final output is ordinary SQL data; any partial below it belongs to another aggregate.
      case agg: CometBaseAggregateExec if agg.modes.contains(Final) => agg
      case agg: BaseAggregateExec if agg.aggregateExpressions.exists(_.mode == Final) => agg
      case placeholder: CometSinkPlaceHolder =>
        val child = restore(placeholder.child)
        if (child eq placeholder.child) placeholder else child
      case other =>
        val children = other.children.map(restore)
        if (children == other.children) {
          other
        } else {
          other match {
            // Only the outer TopK owns Spark's original offset and projection. If a future
            // fused input can contain a restored aggregate, remove its inserted local node.
            case _: CometLocalTopKExec => children.head
            // A native ancestor embeds its old child in nativeOp. Replacing only its SparkPlan
            // child would leave the incompatible native partial in that serialized plan.
            case comet: CometExec =>
              withFallbackReason(comet.originalPlan.withNewChildren(children), reason)
            case _ => other.withNewChildren(children)
          }
        }
    }

    exchange.withNewChildren(Seq(restore(exchange.child)))
  }

  // spotless:off

  /**
   * Tries to transform a Spark physical plan into a Comet plan.
   *
   * This rule traverses bottom-up from the original Spark plan and for each plan node, there
   * are a few cases to consider:
   *
   * 1. The child(ren) of the current node `p` cannot be converted to native
   *   In this case, we'll simply return the original Spark plan, since Comet native
   *   execution cannot start from an arbitrary Spark operator (unless it is special node
   *   such as scan or sink such as shuffle exchange, union etc., which are wrapped by
   *   `CometScanWrapper` and `CometSinkPlaceHolder` respectively).
   *
   * 2. The child(ren) of the current node `p` can be converted to native
   *   There are two sub-cases for this scenario: 1) This node `p` can also be converted to
   *   native. In this case, we'll create a new native Comet operator for `p` and connect it with
   *   its previously converted child(ren); 2) This node `p` cannot be converted to native. In
   *   this case, similar to 1) above, we simply return `p` as it is. Its child(ren) would still
   *   be native Comet operators.
   *
   * After this rule finishes, we'll do another pass on the final plan to convert all adjacent
   * Comet native operators into a single native execution block. Please see where
   * `convertBlock` is called below.
   *
   * Here are a few examples:
   *
   *     Scan                       ======>             CometScan
   *      |                                                |
   *     Filter                                         CometFilter
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     Exchange                                       CometExchange
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *
   * Native execution doesn't necessarily have to start from `CometScan`:
   *
   *     Scan                       =======>            CometScan
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *      |                                                |
   *     HashAggregate                                  HashAggregate
   *      |                                                |
   *     Exchange                                       CometExchange
   *      |                                                |
   *     HashAggregate                                  CometHashAggregate
   *      |                                                |
   *     UnsupportedOperator                            UnsupportedOperator
   *
   * A sink can also be Comet operators other than `CometExchange`, for instance `CometUnion`:
   *
   *     Scan   Scan                =======>          CometScan CometScan
   *      |      |                                       |         |
   *     Filter Filter                                CometFilter CometFilter
   *      |      |                                       |         |
   *        Union                                         CometUnion
   *          |                                               |
   *        Project                                       CometProject
   */
  // spotless:on
  private def transform(plan: SparkPlan): SparkPlan = {
    // Walks the whole plan, so it is lazy: only consulted once a node could be converted.
    lazy val readsInputFileBlock = CometScanRule.readsInputFileBlock(plan)

    if (CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED.get(conf)) {
      tagPartiallyReadTypedDatasetOutputs(plan)
    }

    def convertNode(op: SparkPlan): SparkPlan = op match {
      // Scan marker produced by an optional, out-of-tree scan contrib (e.g. contrib/delta).
      // Matched by trait (no compile-time dependency on the contrib) and present only when that
      // contrib is on the classpath. The marker carries its own serde handler and typically wraps
      // the original, link-bearing scan, so the produced exec's originalPlan keeps its logicalLink
      // with no workaround. If conversion declines, the marker itself falls back to the vanilla
      // Spark scan, so leaving it in the plan is safe.
      case marker: CometContribScanMarker =>
        convertToComet(marker, marker.scanHandler).getOrElse(marker)

      // Fully native scan for V1. CometScanExec must always convert to a native scan; the JVM
      // fallback path has been removed. If conversion fails, fall back to the original Spark scan.
      case scan: CometScanExec =>
        convertToComet(scan, CometNativeScan).getOrElse(scan.wrapped)

      // Fully native Iceberg scan for V2 (iceberg-rust path)
      // Only handle scans with native metadata; other scans fall through to isCometScan
      // Config checks (COMET_ICEBERG_NATIVE_ENABLED, COMET_EXEC_ENABLED) are done in CometScanRule
      case scan: CometBatchScanExec if scan.nativeIcebergScanMetadata.isDefined =>
        convertToComet(scan, CometIcebergNativeScan).getOrElse(scan)

      // A declined native CSV scan falls back to Spark's scan, which produces rows.
      case scan: CometBatchScanExec if scan.wrapped.scan.isInstanceOf[CSVScan] =>
        convertToComet(scan, CometCsvNativeScanExec).getOrElse(scan.wrapped)

      // Comet JVM + native scan for V1 and V2
      case op if isCometScan(op) =>
        convertToComet(op, CometScanWrapper).getOrElse(op)

      case scan: InMemoryTableScanExec =>
        val serializer = scan.relation.cacheBuilder.serializer
        val usesCometCacheSerializer = serializer.isInstanceOf[ArrowCachedBatchSerializer]
        val cometCacheFormat = CometExecRule.readsCometCacheFormat(scan)
        val nativeCacheEnabled = CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.get(conf)
        // Walks the cached plan, so it is lazy: only consulted once the native scan is otherwise
        // possible. See CometInMemoryTableScanExec.recordsObservedMetrics.
        lazy val recordsObservedMetrics =
          CometInMemoryTableScanExec.recordsObservedMetrics(scan.relation)
        val nativeScan = nativeCacheEnabled && cometCacheFormat && !recordsObservedMetrics

        if (nativeScan) {
          convertToComet(scan, CometInMemoryTableScanExec).getOrElse(scan)
        } else {
          // The native cache scan is not available for this relation. Record why, then take the
          // same SparkToColumnar fallback that any other unsupported operator would take, so
          // that turning the feature on is never worse for a scan than leaving it off.
          if (nativeCacheEnabled && !usesCometCacheSerializer) {
            withFallbackReason(
              scan,
              s"Comet in-memory cache requires ${classOf[ArrowCachedBatchSerializer].getName} " +
                s"but this relation was cached with ${serializer.getClass.getName}")
          } else if (nativeCacheEnabled && !cometCacheFormat) {
            val unsupported = scan.relation.output
              .filterNot(a => ArrowCachedBatchSerializer.supportsType(a.dataType))
              .map(a => s"${a.name}: ${a.dataType.simpleString}")
            withFallbackReason(
              scan,
              "Comet in-memory cache does not support the type of these cached columns, so the " +
                s"relation was cached in Spark's default format: ${unsupported.mkString(", ")}")
          } else if (nativeCacheEnabled && recordsObservedMetrics) {
            withFallbackReason(
              scan,
              "Comet in-memory cache does not scan a relation whose cached plan records " +
                "Dataset.observe metrics, because Spark collects those metrics only through " +
                "InMemoryTableScanExec")
          } else if (usesCometCacheSerializer) {
            withFallbackReason(
              scan,
              "Native support for operator InMemoryTableScanExec is disabled. " +
                s"Set ${CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key}=true to enable it.")
          }

          if (shouldApplySparkToColumnar(conf, scan, readsInputFileBlock)) {
            convertToComet(scan, CometSparkToColumnarExec).getOrElse(scan)
          } else {
            scan
          }
        }

      // For AQE table-cache stage (Spark 3.5+) on a Comet cache scan. The operators above it are
      // planned again once it materializes, and like a Comet shuffle stage it is a native input.
      case s: QueryStageExec if s.plan.isInstanceOf[CometInMemoryTableScanExec] =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // A CometSparkToColumnarExec from an earlier pass, which AQE reuses over a table-cache stage
      // because it carries its scan's logical link. Wrap it again so re-planned parents convert.
      case c: CometSparkToColumnarExec =>
        convertToComet(c, CometScanWrapper).getOrElse(c)

      // A leaf with its own enabled Comet operator, such as RangeExec, uses that operator. The
      // Spark-to-Arrow conversion is the fallback for a leaf the operator declines.
      case op: LeafExecNode if hasEnabledHandler(op) =>
        convertToComet(op, allExecs(op.getClass))
          .orElse {
            if (shouldApplySparkToColumnar(conf, op, readsInputFileBlock)) {
              convertToComet(op, CometSparkToColumnarExec)
            } else {
              None
            }
          }
          .getOrElse(op)

      case op if shouldApplySparkToColumnar(conf, op, readsInputFileBlock) =>
        convertToComet(op, CometSparkToColumnarExec).getOrElse(op)

      // Typed Dataset operations (`map`, `flatMap`, `mapPartitions`, `mapGroups`, ...) pass JVM
      // objects between their operators, so those stay on Spark. Each of them ends in
      // `SerializeFromObjectExec`, though, whose output is ordinary rows, and converting those to
      // Arrow lets the operators above the typed operation run natively.
      case op: SerializeFromObjectExec
          if CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED.get(conf) =>
        op.getTagValue(CometExecRule.TYPED_DATASET_PARTIAL_READER) match {
          case Some(reader) =>
            withFallbackReason(
              op,
              "Comet does not convert the output of a typed Dataset operation when " +
                s"$reader can stop reading it early, because filling an Arrow batch would " +
                "run the user function on rows that Spark never reaches")
          // The conversion reads ahead of input_file_name and friends, as it does over a leaf.
          // See shouldApplySparkToColumnar.
          case None if readsInputFileBlock =>
            withFallbackReason(op, CometExecRule.INPUT_FILE_BLOCK_FALLBACK_REASON)
          case None =>
            convertTypedDatasetOutput(op)
        }

      // Spark 4.0+: replace only the per-task write, leaving DataWritingCommandExec - and
      // therefore Spark's commit protocol, stats trackers and SaveMode handling - in place.
      // `V1WritesUtils.getWriteFilesOpt` matches the `WriteFilesExecBase` trait there, which is
      // what lets a Comet node stand in for the write node. See CometWriteFilesExec.
      //
      // Matched at the command rather than at the write node so that the output path, which
      // `WriteFilesExec` does not carry, comes straight from the command that owns it. Converting
      // the child from here also means a `WriteFilesExec` without its enclosing command - which
      // nothing produces today - is simply left on Spark instead of being converted against a
      // stale or missing tag.
      case d @ DataWritingCommandExec(cmd: InsertIntoHadoopFsRelationCommand, w: WriteFilesExec)
          if isSpark40Plus =>
        w.setTagValue(CometExecRule.WRITE_OUTPUT_PATH, cmd.outputPath.toString)
        d.withNewChildren(Seq(convertToComet(w, CometWriteFiles).getOrElse(w)))

      // Spark 3.x: `getWriteFilesOpt` matches the concrete `WriteFilesExec` case class, so a
      // Comet node can never stand in for the write node. Native writes instead replace the whole
      // DataWritingCommandExec and re-implement the write framework inside CometNativeWriteExec.
      // This path is retained only for 3.4/3.5 and goes away with them.
      //
      // `originalPlan` is that command. This rule copies `originalPlan.logicalLink` onto the
      // Comet node, so AQE re-plans the write with the command rather than with whatever child
      // happened to sit under it. No second DataWritingCommandExec is inserted on top.
      case op: DataWritingCommandExec if !isSpark40Plus =>
        convertToComet(op, CometDataWritingCommand).getOrElse(op)

      // `originalPlan` is this IcebergWriteExec, so AQE re-plans the write as this node.
      // A shuffle directly under the native write stays in the child stage and is not wrapped
      // again.
      case op: IcebergWriteExec if CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.get(op.conf) =>
        convertToComet(op, CometIcebergNativeWrite).getOrElse(op)

      // For AQE broadcast stage on a Comet broadcast exchange
      case s @ BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      case s @ BroadcastQueryStageExec(
            _,
            ReusedExchangeExec(_, _: CometBroadcastExchangeExec),
            _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // `CometBroadcastExchangeExec`'s broadcast output is not compatible with Spark's broadcast
      // exchange. It is only used for Comet native execution. We only transform Spark broadcast
      // exchange to Comet broadcast exchange if its downstream is a Comet native plan or if the
      // broadcast exchange is forced to be enabled by Comet config.
      case plan if plan.children.exists(_.isInstanceOf[BroadcastExchangeExec]) =>
        val newChildren = plan.children.map {
          // Tagged by CometSpark34AqeDppFallbackRule on Spark < 3.5 to keep the build-side
          // broadcast Spark-native so Spark's PlanAdaptiveDynamicPruningFilters can match it.
          case b: BroadcastExchangeExec
              if b.getTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG).isDefined =>
            b
          case b: BroadcastExchangeExec if b.children.forall(_.isInstanceOf[CometNativeExec]) =>
            convertToComet(b, CometBroadcastExchangeExec).getOrElse(b)
          case other => other
        }
        if (!newChildren.exists(_.isInstanceOf[BroadcastExchangeExec])) {
          val newPlan = convertNode(plan.withNewChildren(newChildren))
          if (isCometNative(newPlan) || CometConf.COMET_EXEC_BROADCAST_FORCE_ENABLED.get(conf)) {
            newPlan
          } else {
            // copy fallback reasons to the original plan
            newPlan
              .getTagValue(CometExplainInfo.FALLBACK_REASONS)
              .foreach(reasons => withFallbackReasons(plan, reasons))
            // The join stays on Spark. Any #6008 build-side bridge we inserted is left under a
            // plain Spark BroadcastExchangeExec here; clearWastedBuildSideBridges undoes it after
            // the transform. See issue #6008.
            plan
          }
        } else {
          plan
        }

      // For AQE shuffle stage on a Comet shuffle exchange
      case s @ ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      // For AQE shuffle stage on a reused Comet shuffle exchange
      // Note that we don't need to handle `ReusedExchangeExec` for non-AQE case, because
      // the query plan won't be re-optimized/planned in non-AQE mode.
      case s @ ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) =>
        convertToComet(s, CometExchangeSink).getOrElse(s)

      case s: ShuffleExchangeExec if shouldSkipCometShuffle(s) =>
        preserveSparkAggregateBuffers(s)

      case s: ShuffleExchangeExec if CometShuffleExchangeExec.convertsInputForNativeShuffle(s) =>
        convertShuffleInput(s)

      case s: ShuffleExchangeExec =>
        convertToComet(s, CometShuffleExchangeExec)
          .getOrElse(preserveSparkAggregateBuffers(s))

      case op =>
        // if all children are native (or if this is a leaf node) then see if there is a
        // registered handler for creating a fully native plan
        if (op.children.forall(_.isInstanceOf[CometNativeExec])) {
          val handler = allExecs
            .get(op.getClass)
            .map(_.asInstanceOf[CometOperatorSerde[SparkPlan]])
          handler match {
            case Some(handler) =>
              return convertToComet(op, handler).getOrElse(op)
            case _ =>
          }
        }

        op match {
          case _: CometPlan | _: AQEShuffleReadExec | _: BroadcastExchangeExec |
              _: BroadcastQueryStageExec | _: AdaptiveSparkPlanExec | _: ExecutedCommandExec |
              _: V2CommandExec =>
            // Some execs should never be replaced. We include
            // these cases specially here so we do not add a misleading 'info' message.
            op
          case _: ColumnarToRowTransition =>
            // A transition does no work of its own. This rule only meets one that
            // `convertTypedDatasetOutput` or `convertShuffleInput` inserted on an earlier pass
            // over the same plan.
            op
          case _: WriteFilesExec =>
            // The write is converted at the enclosing DataWritingCommandExec above: on Spark 3.x
            // by replacing the whole command, on 4.0+ by converting this child from there.
            // Tagging it here would produce a spurious "WriteFilesExec is not supported" fallback
            // reason (and a warning when COMET_EXPLAIN_FALLBACK_LOG_ENABLED=true) even when the
            // write is fully native; where the write really did fall back on 4.0+, the node
            // already carries the reason CometWriteFiles gave.
            op
          case d: DataWritingCommandExec
              if isSpark40Plus && d.child.isInstanceOf[CometWriteFilesExec] =>
            // On Spark 4.0+ DataWritingCommandExec is deliberately left in the plan even for a
            // fully native write - Comet replaces only its WriteFilesExec child - so tagging it
            // would report an accelerated write as a fallback. A write whose child was not
            // converted still falls through to the default case below and gets a reason.
            op
          case _ =>
            // The operator was not converted to a Comet plan and no serde handler claimed it, so
            // Comet simply has no support for it. (Operators that do have a handler are reported
            // by `reportUnexplainedFallback` inside `convertToComet`, which is also where the
            // strict check lives - it would be wrong to demand a specific reason here, because
            // nothing ever attempted this operator.) Possible reasons for reaching this point:
            // 1. Comet does not support this operator.
            // 2. The operator could not be supported based on query context and current
            //    configs. In this case, it should have already been tagged with fallback
            //    reasons.
            // 3. The operator has children that could not be converted, so execution
            //    has already fallen back to Spark.
            if (op.children.forall(_.isInstanceOf[CometNativeExec]) && !hasFallbackReason(op)) {
              withFallbackReason(op, s"${op.nodeName} is not supported")
            } else {
              op
            }
        }
    }

    plan.transformUp { case op =>
      val converted = convertNode(refreshStaleShuffleScans(op))
      // Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in DPP expressions
      // when the broadcast child has a Comet plan underneath. This enables exchange reuse
      // between the DPP subquery and the join's CometBroadcastExchangeExec because both
      // will have the same CometBroadcastExchangeExec type and canonical form.
      convertSubqueryBroadcasts(convertWrappedSubqueryBroadcasts(converted))
    }
  }

  /**
   * AQE re-plans around a materialized stage by reusing the physical node linked to it, so a
   * native operator that shares its logical node with a shuffle stage (the final aggregate of a
   * two-phase aggregate) keeps the native plan it got while that input was a bare exchange, read
   * through a plain `Scan`. Once the input is a sink that reads the shuffle directly, its
   * `ShuffleScan` takes the place of the stale leaf. The leaf is patched in place because
   * converting the node again from `originalPlan` would drop the stage's logical link that AQE
   * relies on and re-run serde on a node that is already planned.
   *
   * The leaf is patched when a query stage or the final plan is prepared, not when AQE re-plans.
   * AQE adopts a re-planned plan that differs from its current one, after which it cannot link a
   * stage built over the reused node back into its logical plan. A broadcast stage it cannot link
   * no longer fixes the join's build side, so the next re-plan can move the broadcast to the
   * other side and lose the exchange that dynamic partition pruning reuses.
   */
  private def refreshStaleShuffleScans(op: SparkPlan): SparkPlan = op match {
    case _ if queryStagePrep || scansChildAsSeparateBlock(op) => op
    case native: CometNativeExec if native.children.nonEmpty =>
      refreshedNativeOp(native) match {
        case Some(newOp) =>
          val refreshed = native.withRefreshedNativeOp(newOp)
          // An operator that does not hold its native plan as a field cannot take a new one.
          if (refreshed.nativeOp eq newOp) refreshed else op
        case None => op
      }
    case _ => op
  }

  /**
   * The native plan of `native` with each `Scan` leaf whose input is now a `ShuffleScan` of the
   * same field types replaced by that `ShuffleScan`, or None if there is no such leaf or the plan
   * children cannot be matched to the leaves. In the second case `native` gets an info message
   * for extended explain, since the block still reads its shuffle through the JVM.
   */
  private def refreshedNativeOp(native: CometNativeExec): Option[Operator] = {
    // The native node that feeds each plan child's leaves. A read that AQE put between a stage
    // and the operator planned over it is fed by the stage's sink, whose `ShuffleScan` then reads
    // the partitions that the read specifies.
    val feeders = native.children.map {
      case read: AQEShuffleReadExec =>
        read.child match {
          case sink: CometSinkPlaceHolder if sink.nativeOp.hasShuffleScan => Some(sink)
          case _ => None
        }
      case child: CometNativeExec => Some(child)
      case _ => None
    }
    if (feeders.exists(_.isEmpty)) return None
    val children = feeders.flatten
    // Only a sink that reads a shuffle directly, or a native child that may hold one, can feed
    // a `ShuffleScan`.
    val mayFeedShuffleScan = children.exists {
      case sink: CometSinkPlaceHolder => sink.nativeOp.hasShuffleScan
      case _ => true
    }
    if (!mayFeedShuffleScan) return None
    val leaves = CometExec.nativeLeaves(native.nativeOp)
    if (!leaves.exists(_.hasScan)) return None

    // Each plan child feeds a run of leaves, in order: a sink feeds one, and a native child
    // feeds the leaves of its own native plan.
    val current = children.flatMap {
      case sink: CometSinkPlaceHolder => Seq(sink.nativeOp)
      case child => CometExec.nativeLeaves(child.nativeOp)
    }
    def isStale(leaf: Operator, input: Operator): Boolean = leaf.hasScan && input.hasShuffleScan
    val stale = leaves.zip(current).filter { case (leaf, input) => isStale(leaf, input) }
    if (stale.isEmpty) return None
    val isRefreshable = current.length == leaves.length &&
      stale.forall { case (leaf, input) =>
        leaf.getScan.getFieldsList == input.getShuffleScan.getFieldsList
      }
    if (!isRefreshable) {
      withInfo(native, CometExecRule.STALE_SCAN_KEPT)
      return None
    }
    val inputs = current.iterator
    Some(mapLeaves(native.nativeOp) { leaf =>
      val input = inputs.next()
      if (isStale(leaf, input)) input else leaf
    })
  }

  /** `op` with `f` applied to each childless operator, in `CometExec.nativeLeaves` order. */
  private def mapLeaves(op: Operator)(f: Operator => Operator): Operator =
    if (op.getChildrenCount == 0) {
      f(op)
    } else {
      val children = op.getChildrenList.asScala.map(mapLeaves(_)(f))
      op.toBuilder.clearChildren().addAllChildren(children.asJava).build()
    }

  /**
   * Whether `op` is a writer whose native plan builds its own `Scan` over its child, so that the
   * child runs as a separate native block instead of inside that plan.
   */
  private def scansChildAsSeparateBlock(op: SparkPlan): Boolean = op match {
    case _: CometNativeWriteExec | _: CometIcebergWriteExec | _: CometWriteFilesExec => true
    case _ => false
  }

  /**
   * Replace SubqueryBroadcastExec with CometSubqueryBroadcastExec in a node's expressions
   * (non-AQE DPP), and wrap SubqueryAdaptiveBroadcastExec in CometSubqueryAdaptiveBroadcastExec
   * (AQE DPP) to protect it from Spark's PlanAdaptiveDynamicPruningFilters.
   *
   * Non-AQE DPP: When CometExecRule converts BroadcastExchangeExec to CometBroadcastExchangeExec
   * on the join side, the DPP subquery still references the original BroadcastExchangeExec.
   * ReuseExchangeAndSubquery (which runs after Comet rules) can't match them because they have
   * different types. By replacing SubqueryBroadcastExec with CometSubqueryBroadcastExec (which
   * wraps a CometBroadcastExchangeExec), both sides have the same exchange type and reuse works.
   *
   * AQE DPP: Spark's PlanAdaptiveDynamicPruningFilters (queryStageOptimizerRule) pattern-matches
   * on SubqueryAdaptiveBroadcastExec. When it can't find BroadcastHashJoinExec (Comet replaced
   * it), it replaces DPP with Literal.TrueLiteral. We wrap SABs in
   * CometSubqueryAdaptiveBroadcastExec to prevent this. CometPlanAdaptiveDynamicPruningFilters (a
   * later queryStageOptimizerRule) unwraps and converts them with access to the materialized
   * BroadcastQueryStageExec.
   */
  private def convertSubqueryBroadcasts(plan: SparkPlan): SparkPlan = {
    // CometIcebergNativeScanExec.runtimeFilters is a top-level constructor field visible to
    // productIterator, so transformExpressionsUp rewrites it directly. The wrapped @transient
    // originalPlan still holds the pre-rewrite runtimeFilters; we don't sync it here because
    // CometIcebergNativeScanExec.serializedPartitionData rebuilds originalPlan from the
    // top-level runtimeFilters at serialization time (single source of truth).
    plan.transformExpressionsUp { case inSub: InSubqueryExec =>
      rewriteInSubqueryPlan(inSub)
    }
  }

  /**
   * A scan inside a `CometScanWrapper` (a scan bridged into Arrow, or a V2 scan Comet wraps) sits
   * in `CometScanWrapper.originalPlan`, which is not a child, so its DPP subqueries are rewritten
   * there. Left as they are, Spark's PlanAdaptiveDynamicPruningFilters finds no Spark broadcast
   * join for the scan's SubqueryAdaptiveBroadcastExec and can replace the pruning with true, so
   * the scan reads every partition.
   */
  private def convertWrappedSubqueryBroadcasts(plan: SparkPlan): SparkPlan = plan match {
    case wrapper: CometScanWrapper =>
      val original = wrapper.originalPlan.transformUp { case node =>
        convertSubqueryBroadcasts(node)
      }
      if (original eq wrapper.originalPlan) {
        wrapper
      } else {
        val rewritten = wrapper.copy(originalPlan = original)
        rewritten.copyTagsFrom(wrapper)
        rewritten
      }
    case other => other
  }

  private def rewriteInSubqueryPlan(inSub: InSubqueryExec): Expression = {
    inSub.plan match {
      case sub: SubqueryBroadcastExec =>
        sub.child match {
          case b: BroadcastExchangeExec =>
            // The BroadcastExchangeExec child is a Comet columnar-to-row transition wrapping
            // a Comet plan. Strip the row transition to get the columnar Comet plan.
            // CometColumnarToRowExec is CodegenSupport, so by the time this rule sees the
            // subquery plan it is compiled into WholeStageCodegenExec(CometColumnarToRowExec(
            // InputAdapter(cometPlan))); CometNativeColumnarToRowExec is not CodegenSupport and
            // sits directly under the exchange.
            val cometChild = b.child match {
              case c2r: CometNativeColumnarToRowExec => c2r.child
              case c2r: CometColumnarToRowExec => c2r.child
              case WholeStageCodegenExec(c2r: CometColumnarToRowExec) =>
                c2r.child match {
                  case InputAdapter(child) => child
                  case other => other
                }
              case other => other
            }
            if (cometChild.isInstanceOf[CometNativeExec]) {
              logInfo(
                "Converting SubqueryBroadcastExec to " +
                  "CometSubqueryBroadcastExec for DPP exchange reuse")
              val cometBroadcast = CometBroadcastExchangeExec(b, b.output, b.mode, cometChild)
              val cometSub = CometSubqueryBroadcastExec(
                sub.name,
                getSubqueryBroadcastExecIndices(sub),
                sub.buildKeys,
                cometBroadcast)
              inSub.withNewPlan(cometSub)
            } else {
              inSub
            }
          case _ => inSub
        }
      case sab: SubqueryAdaptiveBroadcastExec if isSpark35Plus =>
        // Wrap SABs to prevent Spark's PlanAdaptiveDynamicPruningFilters from
        // converting them to Literal.TrueLiteral. Spark's rule pattern-matches for
        // BroadcastHashJoinExec, which Comet replaced with CometBroadcastHashJoinExec.
        // Without wrapping, DPP is disabled for both Comet native scans and non-Comet
        // scans (e.g., V2 BatchScan). CometPlanAdaptiveDynamicPruningFilters
        // (queryStageOptimizerRule, 3.5+) unwraps and converts them later.
        //
        // On Spark 3.4, injectQueryStageOptimizerRule is unavailable. The isSpark35Plus
        // guard leaves SABs unwrapped; CometSpark34AqeDppFallbackRule then tags the
        // matching BHJ's build broadcast so Spark's rule can match it natively.
        assert(
          sab.buildKeys.nonEmpty,
          s"SubqueryAdaptiveBroadcastExec '${sab.name}' has empty buildKeys")
        logInfo(
          s"Wrapping SubqueryAdaptiveBroadcastExec '${sab.name}' in " +
            "CometSubqueryAdaptiveBroadcastExec to preserve AQE DPP")
        val indices = getSubqueryBroadcastIndices(sab)
        val wrapped = CometSubqueryAdaptiveBroadcastExec(
          sab.name,
          indices,
          sab.onlyInBroadcast,
          sab.buildPlan,
          sab.buildKeys,
          sab.child)
        inSub.withNewPlan(wrapped)
      case _ => inSub
    }
  }

  /**
   * A relation keeps the cache format it was stored in, since `spark.sql.cache.serializer` is
   * static, so a plan that runs without Comet's native execution still reads relations cached in
   * Comet's format. Spark's `InMemoryTableScanExec` reads that format more slowly than Spark's
   * own (https://github.com/apache/datafusion-comet/issues/5485), and nothing else records a
   * fallback reason in such a plan, so record one on each scan that does.
   */
  private def explainSparkReadsOfCometCache(plan: SparkPlan, cause: String): Unit =
    plan.foreach {
      case scan: InMemoryTableScanExec if CometExecRule.readsCometCacheFormat(scan) =>
        val _ = withFallbackReason(
          scan,
          s"$cause, so Spark reads this relation from Comet's cache format, which is slower " +
            "than reading Spark's own. Set " +
            s"${CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key}=false when the application " +
            "starts to cache in Spark's format instead.")
      case _ =>
    }

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    // We shouldn't transform Spark query plan if Comet is not loaded.
    if (!isCometLoaded(conf)) {
      explainSparkReadsOfCometCache(plan, "Comet is disabled")
      return plan
    }

    // Comet does not support structured streaming. Fall back to Spark for any plan that
    // belongs to a streaming query (detected via StreamSourceAwareSparkPlan.getStream).
    if (ShimCometStreaming.isStreamingPlan(plan)) return plan

    if (!CometConf.COMET_EXEC_ENABLED.get(conf)) {
      explainSparkReadsOfCometCache(plan, s"${CometConf.COMET_EXEC_ENABLED.key} is false")
      // Comet exec is disabled, but for Spark shuffle, we still can use Comet columnar shuffle
      if (isCometShuffleEnabled(conf)) {
        applyCometShuffle(plan)
      } else {
        plan
      }
    } else {
      // The rewrite removes the sorts of each join it converts, so put back any sort a kept
      // operator above still needs. This runs before transform(), which converts the sorts it
      // adds like any other SortExec.
      val planWithJoinRewritten = if (CometConf.COMET_FORCE_SHJ.get()) {
        RewriteJoin.restoreRequiredOrdering(plan.transformUp { case p =>
          RewriteJoin.rewrite(p, conf)
        })
      } else {
        plan
      }

      // Tag Partial aggregates that must not be converted to Comet because a
      // corresponding Final or PartialMerge cannot be converted and the intermediate buffer
      // formats are incompatible. This runs before transform() so the tags are checked
      // during the bottom-up conversion. Tags persist through AQE stage creation.
      tagUnsafePartialAggregates(planWithJoinRewritten)

      // Tag leaves on broadcast join build sides so shouldApplySparkToColumnar can bridge an
      // unsupported build-side scan (e.g. ORC) to Arrow and let the join run natively (#6008).
      tagBroadcastBuildSideLeaves(planWithJoinRewritten)

      var transformed = transform(planWithJoinRewritten)
      // A build-side bridge is only worth keeping when its broadcast join went native. Where the
      // join stayed on Spark, clearWastedBuildSideBridges clears the tag from those scans and we
      // transform once more so the build side rebuilds exactly as it would without the feature.
      // See issue #6008.
      if (clearWastedBuildSideBridges(transformed)) {
        transformed = transform(planWithJoinRewritten)
      }
      var newPlan = revertUnsafePartialAggregates(transformed)

      // if the plan cannot be run fully natively then explain why (when appropriate
      // config is enabled)
      if (CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get()) {
        val info = new ExtendedExplainInfo()
        if (info.fallbackReasons(newPlan).nonEmpty) {
          logWarning(
            "Comet cannot execute some parts of this plan natively " +
              s"(set ${CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key}=false " +
              "to disable this logging):\n" +
              s"${info.generateExtendedInfo(newPlan)}")
        }
      }

      newPlan = CometExecRule.removePlaceholders(newPlan)

      // Revert CometColumnarShuffle to Spark's ShuffleExchangeExec when both its parent and child
      // are non-Comet HashAggregate/ObjectHashAggregate operators that remained JVM after the main
      // transform pass. See https://github.com/apache/datafusion-comet/issues/4004.
      if (CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.get()) {
        newPlan = revertRedundantColumnarShuffle(newPlan)
      }

      // Set up logical links
      newPlan = newPlan.transform {
        case op: CometExec
            if op
              .getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
              .exists(_.isInstanceOf[LogicalQueryStage]) =>
          // AQE replanning reuses this physical root and links it to the current logical stage.
          // originalPlan can still point to a subtree hidden inside that logical leaf, which
          // AQE cannot replace in the current logical plan. Only preserve a direct stage link,
          // not a link inherited from an ancestor.
          // On the ordinary exchange path, the exchange itself is behind a QueryStageExec
          // leaf and is not visited by this transform.
          // Spark 4.1.3 returns the existing root in LogicalQueryStageStrategy and then calls
          // setLogicalLink from SparkStrategies.plan:
          // scalastyle:off line.size.limit
          // https://github.com/apache/spark/blob/v4.1.3/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/LogicalQueryStageStrategy.scala#L64-L65
          // https://github.com/apache/spark/blob/v4.1.3/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L78-L87
          // scalastyle:on line.size.limit
          op
        case op: CometExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
        case op: CometShuffleExchangeExec =>
          // Original Spark shuffle exchange operator might have empty logical link.
          // But the `setLogicalLink` call above on downstream operator of
          // `CometShuffleExchangeExec` will set its logical link to the downstream
          // operators which cause AQE behavior to be incorrect. So we need to unset
          // the logical link here.
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op

        case op: CometBroadcastExchangeExec =>
          if (op.originalPlan.logicalLink.isEmpty) {
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
          } else {
            op.originalPlan.logicalLink.foreach(op.setLogicalLink)
          }
          op
      }

      // Convert native execution block by linking consecutive native operators.
      var firstNativeOp = true
      newPlan.transformDown {
        case op: CometNativeExec =>
          val newPlan = if (firstNativeOp) {
            firstNativeOp = false
            op.convertBlock()
          } else {
            op
          }

          // If reaching leaf node, reset `firstNativeOp` to true
          // because it will start a new block in next iteration.
          if (op.children.isEmpty) {
            firstNativeOp = true
          }

          // CometNativeWriteExec / CometIcebergWriteExec are special: they have two separate
          // plans:
          // 1. A protobuf plan (nativeOp) describing the write operation
          // 2. A Spark plan (child) that produces the data to write
          // The serializedPlanOpt is a def that always returns Some(...) by serializing
          // nativeOp on-demand, so the write exec itself doesn't need convertBlock(). However,
          // its child (e.g., CometNativeScanExec, or a CometProject over an AQEShuffleRead)
          // needs its own serialization. Reset the flag so children can start their own native
          // execution blocks.
          if (scansChildAsSeparateBlock(op)) {
            firstNativeOp = true
          }

          newPlan
        case op =>
          firstNativeOp = true
          op
      }
    }
  }

  /** Convert a Spark plan to a Comet plan using the specified serde handler */
  private def convertToComet(op: SparkPlan, handler: CometOperatorSerde[_]): Option[SparkPlan] = {
    val converted = tryConvertToComet(op, handler)
    if (converted.isEmpty) {
      // Comet looked at this operator and declined it, so it stays in the Spark plan. Lift any
      // reasons recorded on its expressions onto the operator itself - see
      // `rollUpFallbackReasons` for why this is needed - and then make sure something was
      // recorded. The order is required, not incidental: `reportUnexplainedFallback` inspects only
      // the operator's own tag, so a reason still sitting on an expression would look like no
      // reason at all and trip the strict check.
      rollUpFallbackReasons(op)
      reportUnexplainedFallback(op)
    }
    converted
  }

  private def tryConvertToComet(
      op: SparkPlan,
      handler: CometOperatorSerde[_]): Option[SparkPlan] = {
    val serde = handler.asInstanceOf[CometOperatorSerde[SparkPlan]]
    if (isOperatorEnabled(serde, op)) {
      // Get the actual data-producing children (unwrap WriteFilesExec).
      val dataProducingChildren = op.children.flatMap {
        case writeFiles: WriteFilesExec => Seq(writeFiles.child)
        case other => Seq(other)
      }
      if (!op.isInstanceOf[CometScanExec] &&
        (op.output ++ dataProducingChildren.flatMap(_.output)).exists(attr =>
          containsVariantType(attr.dataType))) {
        withFallbackReason(
          op,
          "Native operators do not support schemas containing type VariantType")
        return None
      }

      // For operators that require native children (like writes), check if all data-producing
      // children are CometNativeExec. This prevents runtime failures when the native operator
      // expects Arrow arrays but receives non-Arrow data (e.g., OnHeapColumnVector).
      if (serde.requiresNativeChildren && op.children.nonEmpty) {
        if (!dataProducingChildren.forall(_.isInstanceOf[CometNativeExec])) {
          withFallbackReason(
            op,
            "Cannot perform native operation because input is not in Arrow format")
          return None
        }
      }

      val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
      // Which subexpressions Spark evaluates for every row decides where native execution may
      // skip a function's arguments (see `QueryPlanSerde.canShortCircuitNulls`). Spark's
      // generated code for an aggregate shares them whatever
      // spark.sql.subexpressionElimination.enabled says, and other operators only when it is on.
      val nativeOp = QueryPlanSerde.withSharedSubexpressions(
        op.expressions,
        alwaysShared = op.isInstanceOf[BaseAggregateExec]) {
        if (op.children.nonEmpty && op.children.forall(_.isInstanceOf[CometNativeExec])) {
          val childOp = op.children.map(_.asInstanceOf[CometNativeExec].nativeOp)
          childOp.foreach(builder.addChildren)
          serde.convert(op, builder, childOp: _*)
        } else {
          serde.convert(op, builder)
        }
      }
      return nativeOp.map { nativeOp =>
        val exec = serde.createExec(nativeOp, op)
        rollUpInfoMessages(op, exec)
        exec
      }
    }
    None
  }

  /**
   * Lift fallback reasons recorded on `op`'s expression trees onto `op` itself.
   *
   * Extended explain output only walks plan nodes (`ExtendedExplainInfo.sortup` follows
   * `children` / `innerChildren`, never `expressions`), so a reason tagged on an expression is
   * invisible unless something lifts it onto the enclosing operator. This mirrors what
   * [[rollUpInfoMessages]] already does for the informational tags, and replaces the roll-up that
   * used to be hand-written at every serde call site (see
   * https://github.com/apache/datafusion-comet/issues/5230).
   *
   * Only child *expressions* are collected, not child operators: reasons on a child operator are
   * already reachable by the explain traversal via `children`.
   *
   * Called only when `op` was left in the Spark plan, which scopes the roll-up to the operator
   * that actually failed conversion. That matters because some expression instances
   * (`AttributeReference`s, DPP subquery expressions) are shared across operators, so an unscoped
   * roll-up could surface one expression's reason under several unrelated operators.
   *
   * [[reportUnexplainedFallback]] relies on this having run first; the two must not be separated.
   */
  private def rollUpFallbackReasons(op: SparkPlan): Unit = {
    val reasons = op.expressions
      .flatMap(_.collect { case e: Expression => e })
      .flatMap(_.getTagValue(CometExplainInfo.FALLBACK_REASONS))
      .flatten
      .toSet
    if (reasons.nonEmpty) {
      val _ = withFallbackReasons(op, reasons)
    }
  }

  /**
   * Handle an operator that Comet declined without stating why.
   *
   * When every child is already native, Comet had a real opportunity to convert `op`, so the
   * absence of any reason - on `op` or anywhere in its expression trees - means a serde returned
   * `None` and forgot to record one. Under `COMET_STRICT_FALLBACK_REASONS` (enabled for Comet's
   * own test suites) that is a hard failure; otherwise fall back to a generic message so users
   * still see something. The generic message is what used to mask this whole class of bug, which
   * is why the strict check exists.
   *
   * Must run *after* [[rollUpFallbackReasons]] for the same operator. The check reads only `op`'s
   * own tag, because `hasFallbackReason` deliberately does not traverse expressions (it is a
   * planning control signal, not explain output), so an expression-level reason that has not been
   * lifted yet would be mistaken for no reason at all. [[convertToComet]] is the only production
   * caller and keeps the two calls together.
   *
   * Package-visible so `CometExecRuleSuite` can drive the strict failure directly: no serde in
   * the tree reaches this state, which is exactly what the check enforces, so the only way to
   * test it is to construct the shape by hand.
   */
  private[comet] def reportUnexplainedFallback(op: SparkPlan): Unit = {
    if (op.children.forall(_.isInstanceOf[CometNativeExec]) && !hasFallbackReason(op)) {
      if (CometConf.COMET_STRICT_FALLBACK_REASONS.get(op.conf)) {
        throw new IllegalStateException(
          s"Comet did not convert ${op.nodeName} but recorded no fallback reason on the " +
            "operator or any of its expressions. Add a withFallbackReason call stating why " +
            s"conversion failed. Operator:\n$op")
      }
      val _ = withFallbackReason(op, s"${op.nodeName} is not supported")
    }
  }

  /**
   * Lift informational (non-fallback) messages tagged on an operator and its expressions onto the
   * converted Comet plan node so they appear in verbose extended explain output. Expression-level
   * hints would otherwise be invisible because explain only traverses plan nodes, not
   * expressions. `NATIVE_EXPRS` and `CODEGEN_DISPATCH_EXPRS` names across the tree are lifted the
   * same way so that extended explain can report expression coverage; the dispatched ones
   * additionally become one combined info line when `spark.comet.explain.codegen.enabled` is set.
   */
  private def rollUpInfoMessages(op: SparkPlan, exec: SparkPlan): Unit = {
    val allExprs = op.expressions.flatMap(_.collect { case e: Expression => e })

    val infos =
      op.getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse(Set.empty[String]) ++
        CometExplainInfo.collectExprTagValues(allExprs, CometExplainInfo.EXTENSION_INFO)
    infos.foreach(msg => withInfo(exec, msg))

    appendTagValues(
      exec,
      CometExplainInfo.NATIVE_EXPRS,
      CometExplainInfo.collectExprTagValues(allExprs, CometExplainInfo.NATIVE_EXPRS))

    val routedNames =
      CometExplainInfo.collectExprTagValues(allExprs, CometExplainInfo.CODEGEN_DISPATCH_EXPRS)
    appendTagValues(exec, CometExplainInfo.CODEGEN_DISPATCH_EXPRS, routedNames)
    if (routedNames.nonEmpty && CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.get()) {
      val _ =
        withInfo(exec, s"JVM codegen dispatcher: ${routedNames.toSeq.sorted.mkString(", ")}")
    }
  }

  private def isOperatorEnabled(
      handler: CometOperatorSerde[SparkPlan],
      op: SparkPlan): Boolean = {
    val opName = op.getClass.getSimpleName
    if (handler.enabledConfig.forall(_.get(op.conf))) {
      handler.getSupportLevel(op) match {
        case Unsupported(notes) =>
          withFallbackReason(op, notes.getOrElse(""))
          false
        case Incompatible(notes) =>
          val allowIncompat = CometConf.isOperatorAllowIncompat(opName)
          val incompatConf = CometConf.getOperatorAllowIncompatConfigKey(opName)
          if (allowIncompat) {
            if (notes.isDefined) {
              logWarning(
                s"Comet supports $opName when $incompatConf=true " +
                  s"but has notes: ${notes.get}")
            }
            true
          } else {
            val optionalNotes = notes.map(str => s" ($str)").getOrElse("")
            withFallbackReason(
              op,
              s"$opName is not fully compatible with Spark$optionalNotes. " +
                s"To enable it anyway, set $incompatConf=true. " +
                s"${CometConf.COMPAT_GUIDE}.")
            false
          }
        case Compatible(notes, _) =>
          if (notes.isDefined) {
            logWarning(s"Comet supports $opName but has notes: ${notes.get}")
          }
          true
      }
    } else {
      withFallbackReason(
        op,
        s"Native support for operator $opName is disabled. " +
          s"Set ${handler.enabledConfig.get.key}=true to enable it.")
      false
    }
  }

  private def hasEnabledHandler(op: SparkPlan): Boolean =
    allExecs.get(op.getClass).exists(_.enabledConfig.forall(_.get(op.conf)))

  /**
   * Tags each `SerializeFromObjectExec` whose output an operator above it can stop reading early
   * with that operator, so `transform` does not convert it. Spark computes the rows of a typed
   * Dataset operation one at a time, as the operator above reads them, while the conversion fills
   * a whole Arrow batch first. Below a limit, a `mapPartitions` function such as `_.take(1)`, or
   * code reading `Dataset.rdd`, the conversion would run the user function on rows Spark never
   * reaches, and a function that throws on one of them would fail a query that succeeds in Spark.
   * An operator that reads all of its input before it returns a row ends the search, since Spark
   * computes every row below it anyway: an exchange, a sort, a hash aggregate, or a top-k over
   * input that is not already sorted.
   *
   * Conversion is bottom-up, so this runs first. TreeNode tags survive the child copies made
   * during transformUp, while an identity set would not.
   */
  private def tagPartiallyReadTypedDatasetOutputs(plan: SparkPlan): Unit = {
    def visit(op: SparkPlan, partialReader: Option[String]): Unit = {
      val childReader = op match {
        case serialize: SerializeFromObjectExec =>
          partialReader.foreach(
            serialize.setTagValue(CometExecRule.TYPED_DATASET_PARTIAL_READER, _))
          partialReader
        case _: CollectLimitExec | _: LocalLimitExec | _: GlobalLimitExec => Some("a limit")
        // A top-k reads only its first rows when its input is already sorted.
        case topK: TakeOrderedAndProjectExec
            if SortOrder.orderingSatisfies(topK.child.outputOrdering, topK.sortOrder) =>
          Some("a limit")
        case _: MapPartitionsExec => Some("a mapPartitions function")
        case _: Exchange | _: SortExec | _: HashAggregateExec | _: ObjectHashAggregateExec |
            _: TakeOrderedAndProjectExec =>
          None
        case _ => partialReader
      }
      op.children.foreach(visit(_, childReader))
    }
    // `Dataset.rdd` reads the objects the plan produces, and the RDD's own code decides how many
    // of them to read, as `take(1)` does. The plan's root is the `DeserializeToObjectExec` that
    // `Dataset.rdd` adds. When the Dataset ends in a typed operation such as `map`, Spark's
    // `EliminateSerialization` drops that deserializer together with the operation's serializer,
    // so the root is the operation itself, which produces objects too, or a typed filter or a
    // project over it. A Dataset's own plan ends in rows, so no other plan has such a root.
    def producesObjects(op: SparkPlan): Boolean = op match {
      case _: ObjectProducerExec => true
      case _: FilterExec | _: ProjectExec => producesObjects(op.children.head)
      case _ => false
    }
    visit(plan, if (producesObjects(plan)) Some("code reading Dataset.rdd") else None)
  }

  /**
   * Converts the rows a typed Dataset operation produces to Arrow, so the operators above it can
   * run natively. See [[CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED]].
   *
   * Spark inserts the columnar transitions after this rule, but it does not look below a
   * `RowToColumnarTransition` such as `CometSparkToColumnarExec`. That is harmless above a leaf.
   * Here the typed operation's own operators sit below the conversion, and without a transition
   * they would read a Comet child through `CometExec.doExecute`, Spark's interpreted
   * columnar-to-row path. So the subtree gets its transitions now, from Spark's own rule, and
   * `EliminateRedundantTransitions` later replaces each one over a Comet child with Comet's own.
   * Spark's rule leaves existing transitions alone, which matters because this rule runs over the
   * same plan twice under AQE.
   */
  private def convertTypedDatasetOutput(op: SerializeFromObjectExec): SparkPlan = {
    val unsupported = op.output.filterNot(a =>
      CometSparkToColumnarExec.isTypeSupported(a.dataType, a.name, ListBuffer.empty))
    if (unsupported.nonEmpty) {
      withFallbackReason(
        op,
        "Comet cannot convert the output of a typed Dataset operation to Arrow because it does " +
          "not support the type of these columns: " +
          unsupported.map(a => s"${a.name}: ${a.dataType.simpleString}").mkString(", "))
    } else {
      val withTransitions =
        ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(op)
      convertToComet(withTransitions, CometSparkToColumnarExec).getOrElse(withTransitions)
    }
  }

  /**
   * Converts the rows a Spark operator feeds to a shuffle to Arrow, so that the shuffle runs as
   * native shuffle instead of the JVM columnar shuffle. See
   * [[CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED]], and
   * [[CometShuffleExchangeExec.convertsInputForNativeShuffle]] for when it applies.
   *
   * Spark inserts the columnar transitions after this rule, but it does not look below a
   * `RowToColumnarTransition` such as `CometSparkToColumnarExec`. Without a transition, a Spark
   * operator in the child's subtree that reads a Comet operator would do so through
   * `CometExec.doExecute`, Spark's interpreted columnar-to-row path. So the subtree gets its
   * transitions now, from Spark's own rule, and `EliminateRedundantTransitions` later replaces
   * each one over a Comet child with Comet's own. Spark's rule leaves existing transitions alone,
   * which matters because this rule runs over the same plan twice under AQE.
   */
  private def convertShuffleInput(s: ShuffleExchangeExec): SparkPlan = {
    val child =
      ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(s.child)
    convertToComet(child, CometSparkToColumnarExec)
      .flatMap(converted =>
        convertToComet(s.withNewChildren(Seq(converted)), CometShuffleExchangeExec))
      .getOrElse(
        convertToComet(s, CometShuffleExchangeExec).getOrElse(preserveSparkAggregateBuffers(s)))
  }

  private def shouldApplySparkToColumnar(
      conf: SQLConf,
      op: SparkPlan,
      readsInputFileBlock: => Boolean): Boolean = {
    // A converted leaf reads ahead of the Spark operator that evaluates input_file_name and
    // friends: the conversion fills a whole batch, and Comet operators above it may pull more
    // before they emit. By the time Spark evaluates them, the leaf's reader may have moved on to
    // a later file or unset InputFileBlockHolder at the end of its input, so rows would report
    // another file's values or the unset defaults. Leave the leaf on Spark so that the plan above
    // it stays on Spark too.
    if (!canApplySparkToColumnar(conf, op)) {
      false
    } else if (readsInputFileBlock) {
      withFallbackReason(op, CometExecRule.INPUT_FILE_BLOCK_FALLBACK_REASON)
      false
    } else {
      true
    }
  }

  private def canApplySparkToColumnar(conf: SQLConf, op: SparkPlan): Boolean = {
    // Only consider converting leaf nodes to columnar currently, so that all the following
    // operators can have a chance to be converted to columnar. Leaf operators that output
    // columnar batches, such as Spark's vectorized readers, will also be converted to native
    // comet batches.
    val fallbackReasons = new ListBuffer[String]()
    if (CometSparkToColumnarExec.isSchemaSupported(op.schema, fallbackReasons)) {
      op match {
        // Convert Spark DS v1 scan to Arrow format
        case scan: FileSourceScanExec =>
          scan.relation.fileFormat match {
            case _: CSVFileFormat => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonFileFormat => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetFileFormat => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _: TextFileFormat => shouldConvertTextScan(conf, op)
            // Unsupported format (e.g. ORC): bridge it if the general opt-in is on or it feeds a
            // broadcast build side. The per-format arms above keep honoring their own switches.
            case _ => shouldBridgeUnsupportedScan(conf, op)
          }
        // Convert Spark DS v2 scan to Arrow format
        case scan: BatchScanExec =>
          scan.scan match {
            case _: CSVScan => CometConf.COMET_CONVERT_FROM_CSV_ENABLED.get(conf)
            case _: JsonScan => CometConf.COMET_CONVERT_FROM_JSON_ENABLED.get(conf)
            case _: ParquetScan => CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.get(conf)
            case _: TextScan => shouldConvertTextScan(conf, op)
            case _ => shouldBridgeUnsupportedScan(conf, op)
          }
        case _: RangeExec =>
          isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_RANGE_ENABLED)
        case _: InMemoryTableScanExec =>
          isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_IN_MEMORY_CACHE_ENABLED)
        case _ if ShimCometOneRowRelation.isOneRowRelation(op) =>
          isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_ONE_ROW_RELATION_ENABLED)
        case _: RDDScanExec =>
          isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_RDD_ENABLED)
        case _: RowDataSourceScanExec =>
          isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_ROW_DATA_SOURCE_ENABLED)
        // other leaf nodes
        case _: LeafExecNode =>
          isSparkToArrowEnabled(conf, op)
        case _ =>
          // TODO: consider converting other intermediate operators to columnar.
          false
      }
    } else {
      false
    }
  }

  /**
   * Whether to convert a leaf operator that has a `spark.comet.convert` config of its own. The
   * `spark.comet.sparkToColumnar` settings still convert it too, which is deprecated.
   */
  private def isConversionEnabled(
      conf: SQLConf,
      op: SparkPlan,
      entry: ConfigEntry[Boolean]): Boolean = {
    entry.get(conf) || {
      val deprecated = isSparkToArrowEnabled(conf, op)
      if (deprecated && CometExecRule.warnedDeprecatedConversions.add(entry.key)) {
        logWarning(
          s"Using ${COMET_SPARK_TO_ARROW_ENABLED.key} to convert " +
            s"${Utils.getSimpleName(op.getClass)} to Arrow is deprecated and will stop " +
            s"working in a future major release. Set ${entry.key}=true instead.")
      }
      deprecated
    }
  }

  /**
   * An unsupported file-source scan is bridged to Arrow when the general sparkToColumnar opt-in
   * covers it, or when it feeds a broadcast join's build side (see issue #6008). This is only
   * reached from the unsupported-format arms, so the per-format `COMET_CONVERT_FROM_*` switches
   * for CSV/JSON/Parquet/Text still take precedence.
   */
  private def shouldBridgeUnsupportedScan(conf: SQLConf, op: SparkPlan): Boolean =
    isSparkToArrowEnabled(conf, op) || isBroadcastBuildSideLeaf(op)

  /**
   * A Text scan is converted only by `spark.comet.convert.text.enabled`, like CSV, JSON and
   * Parquet, and so is a Text scan on a broadcast build side: a Text file can hold bytes that are
   * not valid UTF-8, for example a Latin-1 file, and the conversion replaces them with U+FFFD, so
   * results that depend on the original bytes would differ from Spark. A Text build side that the
   * bridge would otherwise convert records why it did not, so the explain output names the
   * config. See issue #7118.
   */
  private def shouldConvertTextScan(conf: SQLConf, op: SparkPlan): Boolean =
    isConversionEnabled(conf, op, CometConf.COMET_CONVERT_FROM_TEXT_ENABLED) || {
      if (isBroadcastBuildSideLeaf(op)) {
        withFallbackReason(op, CometExecRule.TEXT_BUILD_SIDE_FALLBACK_REASON)
      }
      false
    }

  /**
   * True when the build-side auto-bridge should run: its own config is on and Comet broadcast
   * exchange conversion is enabled. Without the latter the `BroadcastExchangeExec` cannot become
   * a `CometBroadcastExchangeExec`, so bridging the build side would only add a row to Arrow copy
   * for no native join. This does not also check the join serdes
   * (COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED / COMET_EXEC_BROADCAST_NESTED_LOOP_JOIN_ENABLED): if
   * the exchange converts but the join does not, `clearWastedBuildSideBridges` clears the tag
   * after the transform and a second transform rebuilds the Spark build side, so a leftover copy
   * never reaches runtime. See issue #6008.
   */
  private def broadcastBuildSideBridgeEnabled: Boolean =
    CometConf.COMET_CONVERT_BROADCAST_BUILD_SIDE_ENABLED.get(conf) &&
      CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.get(conf)

  /**
   * True when `op` is a scan on a broadcast join's build side and build-side bridging is enabled.
   * `tagBroadcastBuildSideLeaves` only sets the tag when bridging is enabled, but the tag lives
   * on the physical node and can survive onto a reused instance, so re-check here: a stale tag
   * must not bridge a scan once the feature is turned off. A tagged scan is bridged to Arrow even
   * when the general sparkToColumnar path is off, letting the build branch and the join run
   * natively. See issue #6008.
   */
  private def isBroadcastBuildSideLeaf(op: SparkPlan): Boolean =
    broadcastBuildSideBridgeEnabled &&
      op.getTagValue(CometExecRule.BROADCAST_BUILD_SIDE_TAG).isDefined

  /**
   * Tag the file-source scan leaves on a broadcast join's build side so
   * shouldApplySparkToColumnar can bridge an unsupported build-side scan (e.g. an ORC scan) to
   * Arrow, letting the whole join run natively. The build side is usually small (auto-broadcasts
   * are capped by the broadcast threshold), so the row to Arrow copy is usually cheap - but that
   * is a heuristic, not a bound: an explicit BROADCAST hint can force a larger build side, and a
   * selective filter above the scan means the bridge copies the pre-filter scan output. See issue
   * #6008.
   *
   * The bridge is applied ONLY when the probe side is already natively scannable, i.e. the join
   * can actually become a fully native CometBroadcastHashJoinExec. Bridging a build side under a
   * join that stays on Spark is pure overhead and, worse, it rewrites the build broadcast's
   * subtree to Comet and breaks Spark's DPP broadcast reuse (the DPP subquery keeps an unbridged
   * copy of the same scan, so the two exchanges no longer share a canonical form). CometScanRule
   * has already run by this point, so a native probe scan is a Comet scan and an unsupported
   * probe scan is still a plain Spark scan - see `hasOnlyNativeScans`.
   *
   * Only `FileSourceScanExec` / `BatchScanExec` are tagged - the scan types the per-format arms
   * of shouldApplySparkToColumnar handle. A natively scannable file (e.g. Parquet) is normally
   * already a `CometScanExec` here; if native scan conversion did not take, its
   * `FileSourceScanExec` is still tagged, but the tag is harmless because the per-format arm
   * decides that scan before isBroadcastBuildSideLeaf is ever consulted. DPP subquery broadcasts
   * live in expressions, not `children`, so `foreach` never reaches them - that scope is
   * intentional.
   */
  private def tagBroadcastBuildSideLeaves(plan: SparkPlan): Unit = {
    if (!broadcastBuildSideBridgeEnabled) {
      return
    }
    plan.foreach {
      case j: BroadcastHashJoinExec =>
        tagBuildIfProbeNative(
          j.buildSide,
          j.left,
          j.right,
          CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED)
      case j: BroadcastNestedLoopJoinExec =>
        tagBuildIfProbeNative(
          j.buildSide,
          j.left,
          j.right,
          CometConf.COMET_EXEC_BROADCAST_NESTED_LOOP_JOIN_ENABLED)
      case _ =>
    }
  }

  /**
   * Tag the build side's file-source scans, but only when the join looks like it can become a
   * native Comet broadcast join: the probe side is natively scannable and the join's own serde is
   * enabled. This is a best-effort gate that avoids inserting a bridge we would only tear down
   * again - it cannot see every reason a join stays on Spark (an unsupported operator above a
   * native probe scan, or a join type the serde declines), so `clearWastedBuildSideBridges` is
   * the backstop that undoes any bridge left under a Spark `BroadcastExchangeExec` after the
   * transform. Skipping here still matters because a bridge under a Spark broadcast breaks
   * Spark's DPP broadcast reuse until it is torn down (the DPP subquery keeps an unbridged copy).
   * We skip when:
   *   - the join's own serde is disabled (`joinEnabled`), so it will stay on Spark; or
   *   - the build-side `BroadcastExchangeExec` carries `SKIP_COMET_BROADCAST_TAG`, which
   *     `CometSpark34AqeDppFallbackRule` sets on Spark 3.4 to keep the broadcast Spark-native so
   *     `PlanAdaptiveDynamicPruningFilters` can match it (bridging it would turn DPP off). See
   *     issue #6008.
   */
  private def tagBuildIfProbeNative(
      buildSide: BuildSide,
      left: SparkPlan,
      right: SparkPlan,
      joinEnabled: ConfigEntry[Boolean]): Unit = {
    if (!joinEnabled.get(conf)) {
      return
    }
    val (buildPlan, probePlan) = buildSide match {
      case BuildLeft => (left, right)
      case BuildRight => (right, left)
    }
    val skipForDpp = buildPlan match {
      case b: BroadcastExchangeExec =>
        b.getTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG).isDefined
      case _ => false
    }
    if (!skipForDpp && hasOnlyNativeScans(probePlan)) {
      tagBuildSideScans(buildPlan)
    }
  }

  /**
   * Tag the broadcast dimension's own file-source scans, descending through its filters, projects
   * and aggregations (incl. the aggregate's shuffle) but stopping at a nested join. A nested
   * join's inputs are not the bounded broadcast dimension - its streamed side can be far larger
   * than the broadcast output - so bridging them would copy a large input to Arrow for no reason.
   * That nested join is tagged by its own pre-pass visit based on its own probe. See issue #6008.
   */
  private def tagBuildSideScans(plan: SparkPlan): Unit = plan match {
    case scan @ (_: FileSourceScanExec | _: BatchScanExec) =>
      scan.setTagValue(CometExecRule.BROADCAST_BUILD_SIDE_TAG, ())
    case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec | _: ShuffledHashJoinExec |
        _: SortMergeJoinExec =>
    case other => other.children.foreach(tagBuildSideScans)
  }

  /**
   * True when `plan` has at least one scan and none of its scan leaves is an unsupported plain
   * Spark scan - i.e. every scan is already a Comet scan (CometScanRule ran before this rule).
   * Used to decide whether a broadcast join's probe side can feed a native join. A probe that
   * itself contains an unsupported scan (e.g. a DSv2 `InMemoryTableWithV2Filter` fact) keeps the
   * join on Spark, so bridging its build side would only break DPP reuse.
   */
  private def hasOnlyNativeScans(plan: SparkPlan): Boolean = {
    var sawNativeScan = false
    var sawUnsupportedScan = false
    plan.foreach {
      case _: CometScanExec | _: CometNativeScanExec | _: CometBatchScanExec =>
        sawNativeScan = true
      case _: FileSourceScanExec | _: BatchScanExec =>
        sawUnsupportedScan = true
      case _ =>
    }
    sawNativeScan && !sawUnsupportedScan
  }

  /**
   * After the main transform, a #6008 build-side bridge is left under a plain Spark
   * `BroadcastExchangeExec` only when its broadcast join did not go native - an unsupported join
   * type (e.g. a full outer `BroadcastNestedLoopJoinExec`), a build side the serde declined, or a
   * probe that is not fully native (e.g. an unsupported operator above a native scan). Clear
   * `BROADCAST_BUILD_SIDE_TAG` from every scan under such a broadcast so a second transform
   * rebuilds that build side exactly as it would without the feature, and so a later AQE stage
   * pass - which prepares the build stage on its own, without the join above it - does not bridge
   * the scan again. Returns true if any tag was cleared.
   *
   * Two guards keep the walk honest. It stops at a `CometBroadcastExchangeExec` - a nested
   * broadcast join that went native keeps its bridge. And it only clears a tag (and so asks for a
   * second transform) when the tagged scan sits under a `CometSparkToColumnarExec`, i.e. it was
   * actually bridged. A CSV/JSON/Parquet/Text scan whose per-format arm left it on Spark still
   * carries the tag but was never bridged, so touching it would force a pointless second
   * transform. The descent enters `CometScanWrapper`'s wrapped `originalPlan` by hand because it
   * is a leaf, so a plain `foreach` would never reach the bridged scan inside it. See issue
   * #6008.
   */
  private def clearWastedBuildSideBridges(plan: SparkPlan): Boolean = {
    var cleared = false
    def clearScanTags(node: SparkPlan, underBridge: Boolean): Unit = node match {
      // A nested broadcast join that went native keeps its bridge.
      case _: CometBroadcastExchangeExec =>
      case w: CometScanWrapper => clearScanTags(w.originalPlan, underBridge)
      case bridge: CometSparkToColumnarExec => clearScanTags(bridge.child, underBridge = true)
      case other =>
        if (underBridge && other.getTagValue(CometExecRule.BROADCAST_BUILD_SIDE_TAG).isDefined) {
          other.unsetTagValue(CometExecRule.BROADCAST_BUILD_SIDE_TAG)
          cleared = true
        }
        other.children.foreach(clearScanTags(_, underBridge))
    }
    plan.foreach {
      case b: BroadcastExchangeExec => clearScanTags(b, underBridge = false)
      case _ =>
    }
    cleared
  }

  private def isSparkToArrowEnabled(conf: SQLConf, op: SparkPlan) = {
    COMET_SPARK_TO_ARROW_ENABLED.get(conf) && {
      val simpleClassName = Utils.getSimpleName(op.getClass)
      val nodeName = simpleClassName.replaceAll("Exec$", "")
      // Without the list, the switch still converts the operators the list named by default.
      val operators =
        if (conf.getConfString(COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.key, null) == null) {
          CometExecRule.deprecatedSparkToArrowOperators
        } else {
          COMET_SPARK_TO_ARROW_SUPPORTED_OPERATOR_LIST.get(conf)
        }
      operators.contains(nodeName)
    }
  }

  /**
   * Walk the plan to find buffer-consuming aggregates that cannot be converted to Comet. If the
   * aggregate functions have incompatible intermediate buffer formats, tag the aggregates that
   * produce those buffers so they will also be skipped during conversion.
   *
   * This prevents the crash described in issue #1389 where a Comet Partial produces intermediate
   * data in a format that the Spark consumer cannot interpret.
   */
  private def tagUnsafePartialAggregates(plan: SparkPlan): Unit = {
    plan.foreach {
      case agg: BaseAggregateExec =>
        // A Final or PartialMerge that consumes an incompatible buffer and cannot itself be
        // converted must not sit above a Comet aggregate that produces that buffer, otherwise
        // Spark would try to read a Comet-encoded buffer and crash. Tag every aggregate between
        // it and the bottom Partial, which doConvert honors in any mode. Tagging only the bottom
        // Partial is not enough: a native PartialMerge decodes a Spark Partial's collect_list /
        // collect_set buffer, so the intermediate PartialMerge stages of a distinct-aggregate
        // rewrite would still convert above a Spark Partial. See issues #1389, #4724 and #4813.
        val modes = agg.aggregateExpressions.map(_.mode).distinct
        val consumesBuffers = modes == Seq(Final) || modes.contains(PartialMerge)
        val consumerMode: AggregateMode =
          if (modes.contains(PartialMerge)) PartialMerge else Final
        if (consumesBuffers &&
          !QueryPlanSerde.allAggsSupportNativePartialToSparkFinal(agg.aggregateExpressions) &&
          !canAggregateBeConverted(agg, consumerMode)) {
          val consumer = if (consumerMode == Final) "final" else "partial-merge"
          findBufferProducersInPlan(agg.child).foreach { producer =>
            val producerMode: AggregateMode =
              if (producer.aggregateExpressions.nonEmpty &&
                producer.aggregateExpressions.forall(_.mode == Partial)) {
                Partial
              } else {
                PartialMerge
              }
            // Only tag if the producer would otherwise have been converted. If it cannot be
            // converted itself (e.g. an incompatible input type or a map-typed grouping key),
            // tagging would mask the natural, more specific fallback reason.
            if (canAggregateBeConverted(producer, producerMode)) {
              val stage = if (producerMode == Partial) "Partial" else "Partial-merge"
              producer.setTagValue(
                CometExecRule.COMET_UNSAFE_PARTIAL,
                s"$stage aggregate disabled: corresponding $consumer aggregate " +
                  "cannot be converted to Comet and intermediate buffer formats are incompatible")
            }
          }
        }
      case _ =>
    }
  }

  /**
   * Inspect a failed repair's buffer path without rewriting it or materializing any stage. Report
   * only a native Partial/PartialMerge whose emitted state is not known to be Spark-compatible.
   * Spark Partials and completed aggregates establish new buffers, so stop there rather than
   * finding an unrelated native producer below them. Only known aggregate, sort and exchange
   * wrappers forward the same buffer path; an arbitrary operator is not evidence of a mixed
   * boundary.
   */
  private def hasUnrepairedNativeBuffer(plan: SparkPlan): Boolean = plan match {
    case agg: CometBaseAggregateExec if agg.aggregateExpressions.isEmpty =>
      hasUnrepairedNativeBuffer(agg.child)
    case agg: CometBaseAggregateExec =>
      agg.modes.forall(m => m == Partial || m == PartialMerge) &&
      !QueryPlanSerde.allAggsSupportNativePartialToSparkFinal(agg.aggregateExpressions)
    case agg: BaseAggregateExec
        if agg.aggregateExpressions.nonEmpty &&
          agg.aggregateExpressions.forall(_.mode == Partial) =>
      false
    case agg: BaseAggregateExec =>
      agg.aggregateExpressions.forall(e => e.mode == Partial || e.mode == PartialMerge) &&
      hasUnrepairedNativeBuffer(agg.child)
    case placeholder: CometSinkPlaceHolder => hasUnrepairedNativeBuffer(placeholder.child)
    // A sort aggregate reads its buffers through the sort that orders them.
    case sort: SortExec => hasUnrepairedNativeBuffer(sort.child)
    case sort: CometSortExec => hasUnrepairedNativeBuffer(sort.child)
    case read: AQEShuffleReadExec => hasUnrepairedNativeBuffer(read.child)
    case stage: ShuffleQueryStageExec => hasUnrepairedNativeBuffer(stage.plan)
    case reused: ReusedExchangeExec => hasUnrepairedNativeBuffer(reused.child)
    case shuffle: CometShuffleExchangeExec => hasUnrepairedNativeBuffer(shuffle.child)
    case shuffle: ShuffleExchangeExec => hasUnrepairedNativeBuffer(shuffle.child)
    case _ => false
  }

  /**
   * The early tagging pass cannot know whether a Final's child will become native. Check the
   * actual conversion result before serialization or AQE stage creation, restoring the feeding
   * aggregate/sort/exchange chain while keeping native work below its Partial. When the Partial
   * is already in Spark, restore the native merge stages above it instead. Return the repaired
   * plan, or preserve an unrepairable path and record one warning on its Spark Final if an unsafe
   * native producer remains. Existing stages and their buffers are never rewritten by this pass.
   */
  private[rules] def revertUnsafePartialAggregates(plan: SparkPlan): SparkPlan = {
    // `belowNativeMerge` is set once the walk has passed a native aggregate that merges buffers.
    def revertChain(node: SparkPlan, belowNativeMerge: Boolean): Option[SparkPlan] = node match {
      case agg: CometBaseAggregateExec if agg.modes == Seq(Partial) =>
        Some(
          restoreSparkPartial(
            agg,
            "Partial aggregate disabled: corresponding final aggregate " +
              "cannot be converted to Comet and intermediate buffer formats are incompatible"))

      case agg: CometBaseAggregateExec
          if agg.modes.forall(m => m == Partial || m == PartialMerge) =>
        val merges = agg.aggregateExpressions.nonEmpty
        revertChain(agg.child, belowNativeMerge || merges).map { child =>
          val restored = agg.originalPlan.withNewChildren(Seq(child))
          // A native PartialMerge decodes a Spark partial's collect_list / collect_set buffer,
          // so without the tag the transform below would convert the restored merge again.
          if (merges) {
            val reason = "Partial-merge aggregate disabled: corresponding final aggregate " +
              "cannot be converted to Comet and intermediate buffer formats are incompatible"
            restored.setTagValue(CometExecRule.COMET_UNSAFE_PARTIAL, reason)
            withFallbackReason(restored, reason)
          } else {
            restored
          }
        }

      case agg: BaseAggregateExec
          if agg.aggregateExpressions.nonEmpty &&
            agg.aggregateExpressions.forall(_.mode == Partial) =>
        // This producer already emits Spark buffers. Do not reach through it to an unrelated
        // aggregate below it, but do restore a native merge above it that reads those buffers.
        if (belowNativeMerge) Some(agg) else None

      case agg: BaseAggregateExec
          if agg.aggregateExpressions.forall(e => e.mode == Partial || e.mode == PartialMerge) =>
        revertChain(agg.child, belowNativeMerge).map(child => agg.withNewChildren(Seq(child)))

      // A sort aggregate reads its buffers through the sort that orders them.
      case sort: CometSortExec =>
        revertChain(sort.child, belowNativeMerge)
          .map(child => sort.originalPlan.withNewChildren(Seq(child)))
      case sort: SortExec =>
        revertChain(sort.child, belowNativeMerge)
          .map(child => sort.withNewChildren(Seq(child)))

      case CometSinkPlaceHolder(_, _, shuffle: CometShuffleExchangeExec) =>
        revertChain(shuffle, belowNativeMerge)
      case shuffle: CometShuffleExchangeExec =>
        revertChain(shuffle.child, belowNativeMerge)
          .map(child => shuffle.originalPlan.withNewChildren(Seq(child)))
      case shuffle: ShuffleExchangeExec =>
        revertChain(shuffle.child, belowNativeMerge)
          .map(child => shuffle.withNewChildren(Seq(child)))

      // Stop at materialized stages and operators outside the feeding aggregate/exchange chain.
      case _ => None
    }

    plan.transformUp {
      case agg: BaseAggregateExec
          if agg.aggregateExpressions.map(_.mode).distinct == Seq(Final) &&
            !QueryPlanSerde.allAggsSupportNativePartialToSparkFinal(agg.aggregateExpressions) =>
        revertChain(agg.child, belowNativeMerge = false)
          // Rebuild native consumers and shuffles from their original Spark operators. Merely
          // replacing their children would leave a native protobuf reading the old buffers.
          .map(child => transform(agg.withNewChildren(Seq(child))))
          .getOrElse {
            if (hasUnrepairedNativeBuffer(agg.child)) {
              val reason = "Comet could not restore a native intermediate buffer producer " +
                "below Spark final aggregate; the remaining buffer may be incompatible"
              // AQE can revisit the same consumer. Record the explanation and warn once,
              // regardless of whether general fallback logging is enabled.
              if (!agg
                  .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                  .exists(_.contains(reason))) {
                if (!CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.get()) logWarning(reason)
                withFallbackReason(agg, reason)
              }
            }
            agg
          }
    }
  }

  /**
   * Look for the bottom Partial-mode aggregate that feeds into the given plan (the child of a
   * Final or PartialMerge), and return it after the aggregates passed on the way down. Walks
   * through exchanges, AQE stages and the sorts that Spark inserts below sort aggregates, and
   * continues down through intermediate aggregate stages whose modes are all Partial /
   * PartialMerge - these are the PartialMerge (and mixed Partial/PartialMerge) stages that
   * Spark's distinct-aggregate rewrite inserts between the Partial and the Final. Stops at
   * anything else, returning nothing if no Partial was reached. Requires
   * `aggregateExpressions.nonEmpty` so that group-by-only dedup stages are traversed rather than
   * mistaken for the partial.
   */
  private def findBufferProducersInPlan(plan: SparkPlan): Seq[BaseAggregateExec] = plan match {
    case agg: BaseAggregateExec
        if agg.aggregateExpressions.nonEmpty &&
          agg.aggregateExpressions.forall(e => e.mode == Partial) =>
      Seq(agg)
    case agg: BaseAggregateExec
        if agg.aggregateExpressions.forall(e => e.mode == Partial || e.mode == PartialMerge) =>
      // Intermediate PartialMerge / mixed stage of a distinct-aggregate rewrite, or a group-by
      // only dedup stage; keep walking down towards the bottom Partial.
      val below = findBufferProducersInPlan(agg.child)
      if (below.isEmpty) below else agg +: below
    case a: AQEShuffleReadExec => findBufferProducersInPlan(a.child)
    case s: ShuffleQueryStageExec => findBufferProducersInPlan(s.plan)
    case e: ShuffleExchangeExec => findBufferProducersInPlan(e.child)
    case s: SortExec => findBufferProducersInPlan(s.child)
    case other =>
      logDebug(
        s"findBufferProducersInPlan: stopping at ${other.nodeName}; not a known passthrough")
      Seq.empty
  }

  /**
   * Conservative check for whether an aggregate could be converted to Comet. Checks operator
   * enablement, grouping expressions, aggregate expressions, and result expressions.
   * Intentionally skips the child-native checks since those depend on transformation state;
   * [[revertUnsafePartialAggregates]] checks the actual conversion result before execution.
   *
   * WARNING: this intentionally mirrors the predicate checks in `CometBaseAggregate.doConvert`
   * (operators.scala). Any change to the convertibility rules there must be reflected here or
   * this tagging pass will drift and either crash (missed tag) or over-disable (spurious tag). A
   * shared predicate helper would be preferable.
   */
  private def canAggregateBeConverted(
      agg: BaseAggregateExec,
      expectedMode: AggregateMode): Boolean = {
    val handler = allExecs.get(agg.getClass)
    if (handler.isEmpty) return false
    val serde = handler.get.asInstanceOf[CometOperatorSerde[SparkPlan]]
    if (!isOperatorEnabled(serde, agg.asInstanceOf[SparkPlan])) return false

    val aggregateExpressions = agg.aggregateExpressions
    val groupingExpressions = agg.groupingExpressions

    if (groupingExpressions.exists(e =>
        SupportLevel.containsType(e.dataType, classOf[MapType]))) {
      return false
    }

    if (!groupingExpressions.forall(e =>
        QueryPlanSerde.exprToProto(e, agg.child.output).isDefined)) {
      return false
    }

    if (aggregateExpressions.isEmpty) {
      // Result expressions always checked when there are no aggregate expressions
      val attributes =
        groupingExpressions.map(_.toAttribute) ++ agg.aggregateAttributes
      return agg.resultExpressions.forall(e =>
        QueryPlanSerde.exprToProto(e, attributes).isDefined)
    }

    val modes = aggregateExpressions.map(_.mode).distinct
    val mixedPartialMerge =
      expectedMode == PartialMerge && modes.toSet == Set(Partial, PartialMerge)
    if (!mixedPartialMerge && modes != Seq(expectedMode)) return false

    // Only Partial binds input attributes; Final and PartialMerge consume intermediate buffers.
    // Mixed distinct-aggregate stages need the same per-expression binding as doConvert.
    if (!aggregateExpressions.forall { e =>
        val binding = e.mode != Final && e.mode != PartialMerge
        QueryPlanSerde.aggExprToProto(e, agg.child.output, binding, agg.conf).isDefined
      }) {
      return false
    }

    // doConvert only checks resultExpressions in Final mode when aggregate expressions exist
    // (Partial emits the buffer directly). Mirror that here to avoid false negatives.
    if (expectedMode == Final) {
      val attributes =
        groupingExpressions.map(_.toAttribute) ++ agg.aggregateAttributes
      agg.resultExpressions.forall(e => QueryPlanSerde.exprToProto(e, attributes).isDefined)
    } else {
      true
    }
  }

}
