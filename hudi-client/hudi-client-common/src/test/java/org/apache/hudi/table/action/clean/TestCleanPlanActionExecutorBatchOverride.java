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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Structural checks on {@link CleanPlanActionExecutor.BatchOverride}. The
 * class exists specifically to keep {@code partitions} and
 * {@code earliestInstant} pinned together so per-batch invocations of the
 * paginated cleaner cannot accidentally drift on the retention boundary
 * mid-run (see BatchOverride javadoc and the spec's correctness
 * requirement).
 *
 * <p>End-to-end verification that {@code CleanPlanActionExecutor.requestClean}
 * honors the override is best done via the integration-test path
 * ({@code TestCleanPlanExecutor} in hudi-spark-client), where a real
 * HoodieTable timeline is available.
 */
public class TestCleanPlanActionExecutorBatchOverride {

  @Test
  void batchOverride_storesFieldsAsProvided() {
    List<String> partitions = Arrays.asList("2026/01/01", "2026/01/02", "2026/01/03");
    HoodieInstant earliest = new HoodieInstant(
        HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20260101010101");

    CleanPlanActionExecutor.BatchOverride override =
        new CleanPlanActionExecutor.BatchOverride(partitions, Option.of(earliest));

    assertSame(partitions, override.partitions,
        "partitions must be stored by reference; the orchestrator owns the slice's lifetime");
    assertNotNull(override.earliestInstant);
    assertEquals("20260101010101", override.earliestInstant.get().getTimestamp());
  }

  @Test
  void batchOverride_acceptsEmptyEarliestInstant() {
    // Tables that have no earlier commits (e.g. brand-new tables) legitimately
    // yield Option.empty from CleanPlanner.getEarliestCommitToRetain. The
    // override must round-trip that unchanged so downstream logic still works.
    CleanPlanActionExecutor.BatchOverride override =
        new CleanPlanActionExecutor.BatchOverride(Arrays.asList("p0"), Option.empty());

    assertNotNull(override.earliestInstant);
    assertEquals(false, override.earliestInstant.isPresent());
  }
}
