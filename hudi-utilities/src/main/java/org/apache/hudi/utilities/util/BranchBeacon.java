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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Names, in the log, which adjudication branch of the v2 metadata-table validator decided a record.
 *
 * <p><b>Why this exists.</b> The validator's decision surface is a table of rows keyed on the state
 * of a file's writing instant before and after the run, and a row is reachable only by arranging a
 * real table into the exact shape it names. A test that asserts a verdict alone cannot tell whether
 * it reached the row it meant to: several rows return the same verdict, so an assertion on
 * {@code DANGLING} passes for at least four of them. The beacon closes that gap — the test asserts
 * the branch id as well as the verdict, so a test that silently starts exercising a different row
 * fails instead of continuing to pass.
 *
 * <p><b>Contract.</b> Each line is exactly {@code MDT_V2_BRANCH=<id>}, at the front of the message
 * and with nothing before it, so {@code grep -o 'MDT_V2_BRANCH=[A-Z0-9_]*'} over a run's log yields
 * the set of branches the run took. Every {@code id} in {@link Branch} is a contract with the tests
 * that grep it and must not be reworded; adding a row means adding an id.
 *
 * <p><b>Cost.</b> One call per adjudicated record, at DEBUG, behind
 * {@link Logger#isDebugEnabled()}. It is off in production by default — per-record logging at INFO
 * is what floods a log aggregator — and a run that wants the trace turns DEBUG on for this one
 * logger rather than for the validator. The verdict and its human-readable detail travel in the
 * report either way; this is the machine-readable half, for tests and for an operator who has
 * already decided to look.
 */
public final class BranchBeacon {

  /**
   * The beacon logger, deliberately its own so DEBUG can be enabled for the branch trace without
   * enabling it for everything else the validator logs.
   */
  private static final Logger LOG = LoggerFactory.getLogger(BranchBeacon.class);

  /** The token every beacon line starts with. Tests grep it; do not reword it. */
  public static final String PREFIX = "MDT_V2_BRANCH=";

  private BranchBeacon() {
  }

  /**
   * Records that {@code branch} decided the record being adjudicated.
   *
   * @param branch the branch that decided; never {@code null}
   */
  public static void hit(Branch branch) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}{}", PREFIX, branch.name());
    }
  }
}
