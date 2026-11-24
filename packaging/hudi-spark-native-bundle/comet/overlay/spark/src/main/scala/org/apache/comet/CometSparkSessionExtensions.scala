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

package org.apache.comet

import java.nio.ByteOrder

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometShuffleManager}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.iceberg.IcebergWriteStrategy
import org.apache.comet.rules.{CometCoalesceShufflePartitions, CometPlanAdaptiveDynamicPruningFilters, CometPropagateEmptyRelation, CometReuseSubquery, CometRule, CometSpark34AqeDppFallbackRule}
import org.apache.comet.shims.ShimCometSparkSessionExtensions

/**
 * CometDriverPlugin will register an instance of this class with Spark.
 *
 * Comet rules are injected into Spark's rule pipeline at several extension points. The execution
 * order differs between AQE and non-AQE paths:
 *
 * Non-AQE (QueryExecution.preparations):
 * {{{
 *   1. PlanDynamicPruningFilters    -- Spark creates non-AQE DPP (SubqueryBroadcastExec)
 *   2. PlanSubqueries               -- Spark creates SubqueryExec for scalar subqueries
 *   3. EnsureRequirements            -- Spark inserts shuffles/sorts
 *   4. ApplyColumnarRulesAndInsertTransitions:
 *      a. preColumnarTransitions:   CometRule (CometScanRule then CometExecRule)
 *         - CometExecRule.convertSubqueryBroadcasts converts SubqueryBroadcastExec to
 *           CometSubqueryBroadcastExec for exchange reuse with Comet broadcasts
 *      b. insertTransitions:        ColumnarToRow/RowToColumnar added
 *      c. postColumnarTransitions:  RevertNativeForTransitionHeavyStages,
 *                                   EliminateRedundantTransitions, CometCacheColumnarRule
 *   5. ReuseExchangeAndSubquery     -- Spark deduplicates subqueries (sees Comet nodes)
 * }}}
 *
 * AQE (AdaptiveSparkPlanExec, Spark 3.5+):
 * {{{
 *   Initial plan:
 *     PlanAdaptiveSubqueries:       creates SubqueryAdaptiveBroadcastExec (SAB) for AQE DPP
 *     queryStagePreparationRules:   CometRule (CometScanRule then CometExecRule)
 *       - CometExecRule.convertSubqueryBroadcasts wraps SABs in
 *         CometSubqueryAdaptiveBroadcastExec to prevent Spark's
 *         PlanAdaptiveDynamicPruningFilters from replacing DPP with Literal.TrueLiteral
 *
 *   Per stage (optimizeQueryStage + postStageCreationRules):
 *     1. queryStageOptimizerRules:
 *        a. PlanAdaptiveDynamicPruningFilters (Spark) -- skips wrapped SABs
 *        b. ReuseAdaptiveSubquery (Spark)
 *        c. OptimizeSkewInRebalancePartitions, CoalesceShufflePartitions,
 *           OptimizeShuffleWithLocalRead (Spark)
 *        d. CometPlanAdaptiveDynamicPruningFilters   -- converts wrapped SABs to
 *           CometSubqueryBroadcastExec with BroadcastQueryStageExec for broadcast reuse
 *        e. CometReuseSubquery                       -- deduplicates converted subqueries
 *        f. CometCoalesceShufflePartitions           -- coalesces the shuffles that Comet
 *           unions and broadcast joins keep Spark's CoalesceShufflePartitions from reaching
 *     2. postStageCreationRules -> ApplyColumnarRulesAndInsertTransitions:
 *        a. preColumnarTransitions: CometRule (no-op, already converted)
 *        b. insertTransitions
 *        c. postColumnarTransitions: RevertNativeForTransitionHeavyStages,
 *                                    EliminateRedundantTransitions, CometCacheColumnarRule
 *
 *   Per re-optimization after a stage materializes (AQEOptimizer, after Spark's batches):
 *     CometPropagateEmptyRelation: treats a grouped Comet aggregate over a stage with no rows
 *     as empty, as AQEPropagateEmptyRelation does for a Spark aggregate
 * }}}
 *
 * On Spark 3.4, injectQueryStageOptimizerRule is unavailable. CometExecRule does not wrap SABs,
 * and CometPlanAdaptiveDynamicPruningFilters, CometReuseSubquery and
 * CometCoalesceShufflePartitions are not registered. AQE DPP scans fall back to Spark so that
 * Spark's PlanAdaptiveDynamicPruningFilters handles them natively (with DPP).
 */
class CometSparkSessionExtensions
    extends (SparkSessionExtensions => Unit)
    with Logging
    with ShimCometSparkSessionExtensions {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // A session can be handed this extension more than once, for example through both
    // spark.sql.extensions and SparkSession.Builder.withExtensions. Injecting twice would run
    // every Comet rule twice per plan.
    if (!CometSparkSessionExtensions.markConfigured(extensions)) {
      logDebug("Comet extension already applied to these session extensions; skipping")
      return
    }
    extensions.injectColumnar { session => CometColumnar(session) }
    // Pre-3.5 only: tag AQE DPP regions so the conversion rules below leave them Spark-native.
    // Registered before CometRule so tags are in place when conversion runs.
    // No-op on Spark 3.5+; see CometSpark34AqeDppFallbackRule's class docstring.
    injectPreSpark35QueryStagePrepRuleShim(extensions, CometSpark34AqeDppFallbackRule)
    extensions.injectQueryStagePrepRule { session =>
      CometRule(session, queryStagePrep = true)
    }
    injectQueryStageOptimizerRuleShim(extensions, CometPlanAdaptiveDynamicPruningFilters)
    injectQueryStageOptimizerRuleShim(extensions, CometReuseSubquery)
    injectQueryStageOptimizerRuleShim(extensions, CometCoalesceShufflePartitions)
    extensions.injectRuntimeOptimizerRule { _ => CometPropagateEmptyRelation }
    extensions.injectPlannerStrategy { session => IcebergWriteStrategy(session) }
  }

  case class CometColumnar(session: SparkSession) extends ColumnarRule {
    override def preColumnarTransitions: Rule[SparkPlan] = CometRule(session)

    override def postColumnarTransitions: Rule[SparkPlan] = {
      val rules = CometRule.postColumnarRules(session)
      plan => rules.foldLeft(plan) { case (p, rule) => rule(p) }
    }
  }
}

object CometSparkSessionExtensions extends Logging {
  lazy val isBigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)
  private val SHUFFLE_MANAGER_KEY = "spark.shuffle.manager"

  /** Session extensions Comet has already been injected into. Weak so sessions can be GC'd. */
  private val configuredExtensions =
    java.util.Collections.synchronizedMap(
      new java.util.WeakHashMap[SparkSessionExtensions, java.lang.Boolean]())

  /** Records that Comet is being injected into `extensions`; false if it already was. */
  private def markConfigured(extensions: SparkSessionExtensions): Boolean =
    configuredExtensions.put(extensions, java.lang.Boolean.TRUE) == null

  /**
   * Checks whether Comet extension should be loaded for Spark.
   */
  private[comet] def isCometLoaded(conf: SQLConf): Boolean = {
    if (isBigEndian) {
      logInfo("Comet extension is disabled because platform is big-endian")
      return false
    }
    if (!COMET_ENABLED.get(conf)) {
      logInfo(s"Comet extension is disabled, please turn on ${COMET_ENABLED.key} to enable it")
      return false
    }

    // CometDriverPlugin makes the same check before registering this extension, but an
    // application can also register the extension directly with spark.sql.extensions. The memory
    // mode comes from the SparkContext's conf, which is what executors use. A session's SQLConf
    // can disagree: when the SparkContext already exists, SparkSession.Builder copies core
    // configs into it without applying them.
    val offHeapEnabled = Option(SparkEnv.get).exists(env => isOffHeapEnabled(env.conf))
    if (!offHeapEnabled && !COMET_ONHEAP_ENABLED.get(conf)) {
      logWarning("Comet extension is disabled because Spark is not running in off-heap mode.")
      return false
    }

    if (COMET_SHUFFLE_ENABLED.get(conf) && !isCometShuffleManagerEnabled) {
      logWarning(
        "Comet extension is disabled because spark.shuffle.manager is not set to " +
          s"${classOf[CometShuffleManager].getName} or " +
          s"${classOf[CometCelebornShuffleManager].getName}. " +
          "Comet provides limited benefit without its shuffle manager. " +
          s"Set ${COMET_SHUFFLE_ENABLED.key}=false to keep Comet enabled with " +
          "Spark's default shuffle manager.")
      return false
    }

    // We don't support INT96 timestamps written by Apache Impala in a different timezone yet
    if (conf.getConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION)) {
      logWarning(
        "Comet extension is disabled, because it currently doesn't support" +
          s" ${SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION} setting to true.")
      return false
    }

    try {
      // This will load the Comet native lib on demand, and if success, should set
      // `NativeBase.loaded` to true
      NativeBase.isLoaded
    } catch {
      case e: Throwable =>
        if (COMET_NATIVE_LOAD_REQUIRED.get(conf)) {
          throw new CometRuntimeException(
            "Error when loading native library. Please fix the error and try again, or fallback " +
              s"to Spark by setting ${COMET_ENABLED.key} to false",
            e)
        } else {
          logWarning(
            "Comet extension is disabled because of error when loading native lib. " +
              "Falling back to Spark",
            e)
        }
        false
    }
  }

  // The shared gate also protects CollectLimit and TakeOrdered, which create single-partition
  // dependencies without passing through ordinary exchange selection. Celeborn requires explicit
  // native opt-in and compatible application settings; local Comet shuffle keeps its behavior.
  def isCometShuffleEnabled(conf: SQLConf): Boolean =
    COMET_SHUFFLE_ENABLED.get(conf) && isCometShuffleManagerEnabled &&
      cometCelebornShuffleFallbackReason(conf, numPartitions = 1).isEmpty

  private def activeCelebornShuffleManager: Option[CometCelebornShuffleManager] =
    Option(SparkEnv.get).flatMap(env => Option(env.shuffleManager)).collect {
      case manager: CometCelebornShuffleManager => manager
    }

  def isCometCelebornShuffleManagerEnabled(conf: SQLConf): Boolean =
    activeCelebornShuffleManager.isDefined ||
      conf.getConfString(SHUFFLE_MANAGER_KEY, "") ==
      classOf[CometCelebornShuffleManager].getName

  // Inspect the manager SparkEnv holds rather than spark.shuffle.manager in the session's
  // SQLConf. The manager is created once for the application, and when the SparkContext already
  // exists, SparkSession.Builder copies core configs into the SQLConf without applying them.
  def isCometShuffleManagerEnabled: Boolean =
    Option(SparkEnv.get).flatMap(env => Option(env.shuffleManager)).exists {
      case _: CometShuffleManager | _: CometCelebornShuffleManager => true
      case _ => false
    }

  /**
   * Native mode and execution can be chosen per query. Encryption, stage recovery, and Celeborn
   * fallback policy belong to the application manager, so session SET commands cannot override
   * their effective values. Inspect the actual manager even if a session changed its class name.
   */
  def cometCelebornShuffleFallbackReason(conf: SQLConf, numPartitions: Int): Option[String] = {
    if (!isCometCelebornShuffleManagerEnabled(conf)) {
      None
    } else if (!COMET_EXEC_ENABLED.get(conf)) {
      Some("Celeborn-backed Comet shuffle requires Comet native execution to be enabled")
    } else if (COMET_SHUFFLE_MODE.get(conf) == "jvm") {
      Some("Celeborn-backed Comet shuffle does not support spark.comet.shuffle.mode=jvm")
    } else if (COMET_SHUFFLE_MODE.get(conf) != "native") {
      Some("Celeborn-backed Comet shuffle requires spark.comet.shuffle.mode=native")
    } else {
      activeCelebornShuffleManager match {
        case Some(manager) => manager.nativeShuffleFallbackReason(numPartitions)
        case None =>
          Some(
            "Celeborn-backed Comet shuffle requires the application's " +
              "CometCelebornShuffleManager")
      }
    }
  }

  def isCometScan(op: SparkPlan): Boolean = {
    op.isInstanceOf[CometBatchScanExec] || op.isInstanceOf[CometScanExec]
  }

  def isSpark35Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "3.5"
  }

  def isSpark40Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.0"
  }

  def isSpark41Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.1"
  }

  def isSpark42Plus: Boolean = {
    org.apache.spark.SPARK_VERSION >= "4.2"
  }

  def isOffHeapEnabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean("spark.memory.offHeap.enabled", false)
  }

  /**
   * Record a fallback reason on a `TreeNode` (a Spark operator or expression) explaining why
   * Comet cannot accelerate it. Reasons recorded here are surfaced in extended explain output
   * (see `ExtendedExplainInfo`) and, when `COMET_EXPLAIN_FALLBACK_LOG_ENABLED` is enabled, logged
   * as warnings.
   *
   * Call this in any code path where Comet decides not to convert a given node - serde `convert`
   * methods returning `None`, unsupported data types, disabled configs, etc. Do not use this for
   * informational messages that are not fallback reasons: anything tagged here is treated by the
   * rules as a signal that the node falls back to Spark.
   *
   * Tag only the node that actually failed, and state a real reason. There is deliberately no way
   * to copy reasons from child nodes onto a parent: extended explain only walks plan nodes, so an
   * expression-level reason is lifted onto the enclosing operator centrally by
   * `CometExecRule.rollUpFallbackReasons` when that operator is left in the Spark plan. See
   * https://github.com/apache/datafusion-comet/issues/5230.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reason. Newline-delimited to record more than one reason.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with the fallback reason attached (as a side effect on its tag map).
   */
  def withFallbackReason[T <: TreeNode[_]](node: T, info: String): T = {
    // support existing approach of passing in multiple infos in a newline-delimited string
    val infoSet = if (info == null || info.isEmpty) {
      Set.empty[String]
    } else {
      info.split("\n").toSet
    }
    withFallbackReasons(node, infoSet)
  }

  /**
   * Record one or more fallback reasons on a `TreeNode`. This is the set-valued form of
   * [[withFallbackReason]]; see that overload for the full contract.
   *
   * Reasons are accumulated (never overwritten) on the node's `FALLBACK_REASONS` tag and are
   * surfaced in extended explain output. When `COMET_EXPLAIN_FALLBACK_LOG_ENABLED` is enabled,
   * each new reason is also emitted as a warning.
   *
   * @param node
   *   The Spark operator or expression that is falling back to Spark.
   * @param info
   *   The fallback reasons for this node.
   * @tparam T
   *   The type of the TreeNode. Typically `SparkPlan`, `AggregateExpression`, or `Expression`.
   * @return
   *   `node` with fallback reasons attached (as a side effect on its tag map).
   */
  def withFallbackReasons[T <: TreeNode[_]](node: T, info: Set[String]): T = {
    if (CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.get()) {
      for (reason <- info) {
        logWarning(s"Comet cannot accelerate ${node.getClass.getSimpleName} because: $reason")
      }
    }
    val existingNodeInfos =
      node.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])
    node.setTagValue(CometExplainInfo.FALLBACK_REASONS, existingNodeInfos ++ info)
    node
  }

  /**
   * True if any fallback reason has been recorded on `node` (via [[withFallbackReason]] /
   * [[withFallbackReasons]]). Callers that need to short-circuit when a prior rule pass has
   * already decided a node falls back can use this as the sticky signal.
   *
   * This deliberately reads only the node's own tag. It is a planning control signal, not explain
   * output, so it must not observe reasons that merely exist somewhere in the node's expression
   * trees - see `CometExecRule.rollUpFallbackReasons`.
   */
  def hasFallbackReason(node: TreeNode[_]): Boolean = {
    node.getTagValue(CometExplainInfo.FALLBACK_REASONS).exists(_.nonEmpty)
  }

  /**
   * Record a purely informational message on a `TreeNode`. Unlike `withFallbackReason`, this does
   * NOT cause the node to fall back to Spark: the planning rules never read this tag. Messages
   * accumulate (never overwrite) on the node's `EXTENSION_INFO` tag and are surfaced in verbose
   * extended explain output under a `[COMET-INFO: ...]` label. Use this to point the user at a
   * faster or alternative path that is available but not currently selected, such as a native
   * implementation gated behind a config.
   */
  def withInfo[T <: TreeNode[_]](node: T, message: String): T = {
    appendTagValue(node, CometExplainInfo.EXTENSION_INFO, message)
  }

  /**
   * Record that `node` (typically an `Expression`) is routing through the JVM codegen dispatcher.
   * `CometExecRule.rollUpInfoMessages` collects the names across an operator's expression trees
   * and emits one combined `[COMET-INFO: ...]` segment.
   */
  def withCodegenDispatchExpr[T <: TreeNode[_]](node: T, name: String): T = {
    appendTagValue(node, CometExplainInfo.CODEGEN_DISPATCH_EXPRS, name)
  }

  /**
   * Record that `node` (typically an `Expression`) was lowered to a native DataFusion expression.
   * The native counterpart of [[withCodegenDispatchExpr]]: `CometExecRule.rollUpInfoMessages`
   * collects the names across an operator's expression trees onto the converted Comet plan node,
   * where extended explain reads them for expression coverage stats.
   */
  def withNativeExpr[T <: TreeNode[_]](node: T, name: String): T = {
    appendTagValue(node, CometExplainInfo.NATIVE_EXPRS, name)
  }

  /**
   * Add `value` to a `Set`-valued `TreeNodeTag`, accumulating rather than overwriting. Null and
   * empty values are dropped so callers do not have to guard. Shared by [[withInfo]] and the
   * expression coverage tags.
   */
  private def appendTagValue[T <: TreeNode[_]](
      node: T,
      tag: TreeNodeTag[Set[String]],
      value: String): T = {
    if (value != null && value.nonEmpty) {
      appendTagValues(node, tag, Set(value))
    }
    node
  }

  /** Bulk form of [[appendTagValue]], for lifting a whole name set onto another node. */
  private[comet] def appendTagValues[T <: TreeNode[_]](
      node: T,
      tag: TreeNodeTag[Set[String]],
      values: Set[String]): T = {
    if (values.nonEmpty) {
      node.setTagValue(tag, node.getTagValue(tag).getOrElse(Set.empty[String]) ++ values)
    }
    node
  }

}
