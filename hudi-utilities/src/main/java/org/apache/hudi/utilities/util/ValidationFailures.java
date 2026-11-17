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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tells a failure of the table apart from a failure of the cluster validating it.
 *
 * <p>Shared by both validation dimensions, which is why it is here rather than on either of them: a
 * duplicate base file and an FS-MDT divergence are different findings, but a decommissioned
 * executor means the same thing to both and must be reported the same way by both.
 */
public final class ValidationFailures {

  /**
   * Stable token for the one log line that says a run died because the cluster went away rather
   * than because the table is inconsistent. Alerting keys off it, so do not reword it.
   */
  public static final String INFRA_FAILURE_MARKER = "MDT_VALIDATOR_INFRA_FAILURE";

  /**
   * Message fragments that identify a failure of the infrastructure running this job — the driver
   * or an executor being torn down, decommissioned or killed — as opposed to a divergence between
   * the metadata table and storage. Matched anywhere in the cause chain.
   *
   * <p>The distinction is not cosmetic: a caller can map a {@link HoodieValidationException}
   * to a data-correctness alert and everything else to an infrastructure alert. A preempted
   * node reported as corruption sends someone to rebuild a metadata table that was never broken.
   */
  private static final List<String> INFRA_FAILURE_SIGNATURES = Collections.unmodifiableList(
      Arrays.asList(
          "cancelled because SparkContext was shut down",
          "Cannot call methods on a stopped SparkContext",
          "SparkContext has been shutdown",
          "ExecutorLostFailure",
          "Executor decommission",
          "decommissioned",
          "Container killed by YARN",
          "Container from a bad node",
          "Executor heartbeat timed out"));

  private ValidationFailures() {
  }

  /**
   * Decides what a failure means, which is what decides whether someone is paged for corruption.
   *
   * <p>Order matters and is deliberate:
   *
   * <ol>
   *   <li>A {@link HoodieValidationException} anywhere in the chain wins, even when the cluster
   *       also went away afterwards. A divergence that was actually observed is a real finding and
   *       a teardown must not bury it.
   *   <li>Otherwise an infrastructure signature makes this a {@link HoodieException}. It is not a
   *       validation failure, the run's {@code hasValidationFailure()} stays false, and the
   *       validation job reports it as an infrastructure error rather than a data-correctness
   *       incident.
   *   <li>Otherwise the failure is passed through as it was.
   * </ol>
   *
   * <p>Spark wraps task-side failures in {@code SparkException}, which is why every step searches
   * the whole cause chain rather than only the top-level throwable.
   */
  public static RuntimeException classifyFailure(Exception e) {
    for (Throwable cause = e; cause != null; cause = cause.getCause()) {
      if (cause instanceof HoodieValidationException) {
        return (HoodieValidationException) cause;
      }
    }
    Option<String> infraReason = matchInfraFailure(e);
    if (infraReason.isPresent()) {
      return new HoodieException(
          INFRA_FAILURE_MARKER + " reason=" + infraReason.get()
              + " - the cluster running this validation went away; the metadata table was not found"
              + " to be inconsistent", e);
    }
    return e instanceof RuntimeException ? (RuntimeException) e : new HoodieException(e);
  }

  /**
   * The infrastructure signature matched by {@code throwable}'s cause chain, if any.
   *
   * @return the matched fragment, for the operator to read in the log line; empty when nothing in
   *         the chain looks like the cluster going away.
   */
  public static Option<String> matchInfraFailure(Throwable throwable) {
    for (String signature : INFRA_FAILURE_SIGNATURES) {
      if (ExceptionUtil.validateErrorMsg(throwable, signature)) {
        return Option.of(signature);
      }
    }
    // A driver shutdown interrupts the threads waiting on the fan-out, and an interrupt carries no
    // message to match on.
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (cause instanceof InterruptedException) {
        return Option.of("InterruptedException");
      }
    }
    return Option.empty();
  }

  /**
   * Whether a failure caught right after a dimension's phase-1 barrier should be discarded as a
   * finding a recompute made unattributable, rather than classified and rethrown.
   *
   * <p>Shared by both dimensions' {@code run()} catch blocks for the reason this class exists at
   * all: an executor lost to infrastructure is simultaneously the commonest cause of a recompute
   * (lost blocks -&gt; a retry recomputes the partition -&gt; the counter bumps) and precisely what
   * {@link #matchInfraFailure} exists to tell apart from real data corruption. Infrastructure is
   * therefore classified <b>first</b>; only when {@code failure} does not match an infrastructure
   * signature is {@code recomputed} consulted. Discarding for a recompute before classifying would
   * let an infra-preempted node report success with {@code MDT_VALIDATOR_RECOMPUTE_SKIPPED} in the
   * log instead of failing as infrastructure -- the exact inversion this method exists to prevent.
   *
   * @param failure    the exception the catch site caught
   * @param recomputed the sealed {@link RecomputeProbe.Baseline#recomputed()} reading for the same
   *                   run
   * @return true when the caller should discard {@code failure} and return an inconclusive
   *         {@code Result}; false when the caller must classify and rethrow {@code failure} instead
   */
  public static boolean discardForRecompute(Throwable failure, boolean recomputed) {
    return recomputed && !matchInfraFailure(failure).isPresent();
  }
}
