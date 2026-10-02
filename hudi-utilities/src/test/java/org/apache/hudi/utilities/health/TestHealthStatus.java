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

package org.apache.hudi.utilities.health;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the severity ordering that table-level verdicts are rolled up with.
 */
public class TestHealthStatus {

  @Test
  public void unhealthyDominatesEveryOtherStatus() {
    // given any pairing with UNHEALTHY
    // then UNHEALTHY wins, so one bad check is never masked by good ones
    assertEquals(HealthStatus.UNHEALTHY, HealthStatus.max(HealthStatus.UNHEALTHY, HealthStatus.HEALTHY));
    assertEquals(HealthStatus.UNHEALTHY, HealthStatus.max(HealthStatus.HEALTHY, HealthStatus.UNHEALTHY));
    assertEquals(HealthStatus.UNHEALTHY, HealthStatus.max(HealthStatus.UNHEALTHY, HealthStatus.SKIPPED));
  }

  @Test
  public void healthyOutranksSkipped() {
    // given a run where one check passed and another was skipped
    // then the table reads HEALTHY: a skipped check is silent, not a failure
    assertEquals(HealthStatus.HEALTHY, HealthStatus.max(HealthStatus.SKIPPED, HealthStatus.HEALTHY));
    assertEquals(HealthStatus.HEALTHY, HealthStatus.max(HealthStatus.HEALTHY, HealthStatus.SKIPPED));
  }

  @Test
  public void allSkippedRollsUpToSkipped() {
    // given a run where nothing could be evaluated
    // then the verdict is SKIPPED rather than a false clean bill of health
    assertEquals(HealthStatus.SKIPPED, HealthStatus.max(HealthStatus.SKIPPED, HealthStatus.SKIPPED));
  }

  @Test
  public void onlyUnhealthyReadsAsUnhealthy() {
    assertTrue(HealthStatus.UNHEALTHY.isUnhealthy());
    assertFalse(HealthStatus.HEALTHY.isUnhealthy());
    assertFalse(HealthStatus.SKIPPED.isUnhealthy());
  }
}
