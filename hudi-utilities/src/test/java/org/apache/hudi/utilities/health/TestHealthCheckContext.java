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

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the fail-fast configuration contract: a check may only reason with writer properties the
 * operator actually supplied, unless defaults were explicitly opted into.
 */
public class TestHealthCheckContext {

  private static final String KEY_A = "hoodie.some.property";
  private static final String KEY_B = "hoodie.other.property";

  private HealthCheckContext contextWith(boolean applyAllDefaults, String... keys) {
    TypedProperties props = new TypedProperties();
    for (String key : keys) {
      props.setProperty(key, "value");
    }
    return new HealthCheckContext(null, props, applyAllDefaults);
  }

  @Test
  public void suppliedConfigsAreUsable() {
    // given a context where both properties were supplied
    HealthCheckContext context = contextWith(false, KEY_A, KEY_B);

    // when asked whether it may reason with them
    // then it may, and nothing is reported missing
    assertTrue(context.hasConfigsOrDefaultsAllowed(KEY_A, KEY_B));
    assertFalse(context.firstMissingConfig(KEY_A, KEY_B).isPresent());
  }

  @Test
  public void missingConfigIsReportedRatherThanDefaulted() {
    // given a context where only one of two required properties was supplied
    HealthCheckContext context = contextWith(false, KEY_A);

    // when asked about both
    // then the unsupplied one is named, rather than silently taking a default
    assertFalse(context.hasConfigsOrDefaultsAllowed(KEY_A, KEY_B));
    assertEquals(KEY_B, context.firstMissingConfig(KEY_A, KEY_B).get());
  }

  @Test
  public void firstMissingConfigNamesTheEarliestGap() {
    // given a context with none of the required properties
    HealthCheckContext context = contextWith(false);

    // when asked about several
    // then the first is named, so the operator has one concrete thing to fix
    assertEquals(KEY_A, context.firstMissingConfig(KEY_A, KEY_B).get());
  }

  @Test
  public void applyAllDefaultsPermitsMissingConfigs() {
    // given a context that explicitly opted into Hudi defaults
    HealthCheckContext context = contextWith(true);

    // when asked about properties nobody supplied
    // then they are treated as available, because the operator accepted the risk
    assertTrue(context.hasConfigsOrDefaultsAllowed(KEY_A, KEY_B));
    assertFalse(context.firstMissingConfig(KEY_A, KEY_B).isPresent());
  }

  @Test
  public void skipResultNamesThePropertyAndBothWaysForward() {
    // given a context missing a property
    HealthCheckContext context = contextWith(false);

    // when a check skips because of it
    HealthCheckResult result = context.skipForMissingConfig("some-check", KEY_A);

    // then the operator is told what is missing and both ways to proceed
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(KEY_A));
    assertTrue(result.getSummary().contains("--props"));
    assertTrue(result.getSummary().contains("--apply-all-defaults"));
  }
}
