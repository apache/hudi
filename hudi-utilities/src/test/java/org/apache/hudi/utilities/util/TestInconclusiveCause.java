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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The two causes a v2 run can reach no verdict for, and what each one tells a caller.
 *
 * <p>The archival cause's strings are an alerting contract, so they are
 * asserted literally rather than derived: a refactor that changed them would silence every alert
 * keyed on them without failing anything else.
 */
class TestInconclusiveCause {

  @Test
  void theArchivalCauseKeepsItsExistingAlertingContract() {
    assertEquals("MDT_VALIDATOR_ARCHIVAL_SKIPPED", InconclusiveCause.ARCHIVAL_RAN.marker());
    assertEquals("Archival ran while validating ", InconclusiveCause.ARCHIVAL_RAN.messagePrefix());
    assertEquals(". The timeline delta the FS-MDT comparison would adjudicate against is therefore "
            + "missing instants that left the active timeline, so that dimension reached no verdict "
            + "and no findings were written for it",
        InconclusiveCause.ARCHIVAL_RAN.messageTail());
    assertEquals(Branch.MTV_ARCHIVAL_EXHAUSTIVE_FAILED,
        InconclusiveCause.ARCHIVAL_RAN.exhaustiveBeacon());
    assertEquals(Branch.MTV_ARCHIVAL_FAIL_FAST_SKIPPED,
        InconclusiveCause.ARCHIVAL_RAN.failFastBeacon());
  }

  @Test
  void theRecomputeCauseIsDistinguishableFromArchivalInEveryChannel() {
    assertEquals("MDT_VALIDATOR_RECOMPUTE_SKIPPED", InconclusiveCause.PHASE_ONE_RECOMPUTED.marker());
    assertNotEquals(InconclusiveCause.ARCHIVAL_RAN.marker(),
        InconclusiveCause.PHASE_ONE_RECOMPUTED.marker());
    assertNotEquals(InconclusiveCause.ARCHIVAL_RAN.messagePrefix(),
        InconclusiveCause.PHASE_ONE_RECOMPUTED.messagePrefix());
    assertNotEquals(InconclusiveCause.ARCHIVAL_RAN.messageTail(),
        InconclusiveCause.PHASE_ONE_RECOMPUTED.messageTail());
    assertEquals(Branch.MTV_RECOMPUTE_EXHAUSTIVE_FAILED,
        InconclusiveCause.PHASE_ONE_RECOMPUTED.exhaustiveBeacon());
    assertEquals(Branch.MTV_RECOMPUTE_FAIL_FAST_SKIPPED,
        InconclusiveCause.PHASE_ONE_RECOMPUTED.failFastBeacon());
  }

  @Test
  void anInconclusiveCarriesBothHalvesAndRefusesNeither() {
    Inconclusive inconclusive =
        new Inconclusive(InconclusiveCause.PHASE_ONE_RECOMPUTED, "2 task(s) re-ran");

    assertEquals(InconclusiveCause.PHASE_ONE_RECOMPUTED, inconclusive.getCause());
    assertEquals("2 task(s) re-ran", inconclusive.getReason());
    assertThrows(IllegalArgumentException.class,
        () -> new Inconclusive(null, "a reason"));
    assertThrows(IllegalArgumentException.class,
        () -> new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, null));
  }
}
