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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPendingServiceReconciler {

  @Test
  void cleanNamedInDltSanctionsTheFile() {
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting()
        .withCleanFreedFile("p", "fg1_0-1-2_100.parquet").build();

    assertEquals(PendingServiceReconciler.Outcome.SANCTIONED,
        PendingServiceReconciler.reconcile("p", "fg1_0-1-2_100.parquet", "100",
            dlt, PendingServiceState.empty()));
  }

  @Test
  void cleanInflightAtPrevAndUnchangedAtAftStillSanctions() {
    // The whole reason pendingAtPrev exists: this clean is in NEITHER end of the state diff,
    // because it did not change. It has still already deleted the file.
    PendingServiceState pending = PendingServiceState.forTesting()
        .withCleanPlannedFile("p", "fg1_0-1-2_100.parquet").build();

    assertEquals(PendingServiceReconciler.Outcome.SANCTIONED,
        PendingServiceReconciler.reconcile("p", "fg1_0-1-2_100.parquet", "100",
            ServiceActivityDelta.forTesting().build(), pending));
  }

  @Test
  void rollbackNamingTheWritingInstantSanctionsTheFile() {
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().withRolledBackInstant("100").build();

    assertEquals(PendingServiceReconciler.Outcome.SANCTIONED,
        PendingServiceReconciler.reconcile("p", ".fg1_100.log.1_0-1-2", "100",
            dlt, PendingServiceState.empty()));
  }

  @Test
  void rollbackNamingTheFileButNotTheInstantStillSanctions() {
    // v6 populates logFilesFromFailedCommit; v8 does not (RollbackHelperV1:318,
    // RollbackHelperFactory:31-35). Either route must work, so no rule may depend on both.
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting()
        .withRollbackFreedFile("p", ".fg1_100.log.1_0-1-2").build();

    assertEquals(PendingServiceReconciler.Outcome.SANCTIONED,
        PendingServiceReconciler.reconcile("p", ".fg1_100.log.1_0-1-2", "999",
            dlt, PendingServiceState.empty()));
  }

  @Test
  void nothingExplainingTheFileIsNaNotUnresolved() {
    assertEquals(PendingServiceReconciler.Outcome.NA,
        PendingServiceReconciler.reconcile("p", "fg1_0-1-2_100.parquet", "100",
            ServiceActivityDelta.forTesting().build(), PendingServiceState.empty()));
  }

  @Test
  void anUnreadablePlanCoveringThisPartitionIsUnresolvedNotNa() {
    // "I could not check" must never become "nothing references this". An NA here would reach
    // DANGLING or a fatal OUTBOUND_DELETION downstream.
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().withReadFailure("p").build();

    assertEquals(PendingServiceReconciler.Outcome.UNRESOLVED,
        PendingServiceReconciler.reconcile("p", "fg1_0-1-2_100.parquet", "100",
            dlt, PendingServiceState.empty()));
  }

  @Test
  void anUnreadablePlanInAnotherPartitionDoesNotBlankThisOne() {
    // Scoped, not global: one corrupt plan must not make every record in the run NOT_SETTLED,
    // because NOT_SETTLED is non-fatal and the run would pass a broken table.
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().withReadFailure("other").build();

    assertEquals(PendingServiceReconciler.Outcome.NA,
        PendingServiceReconciler.reconcile("p", "fg1_0-1-2_100.parquet", "100",
            dlt, PendingServiceState.empty()));
  }
}
