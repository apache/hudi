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

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * How much of a partition's divergence the v2 validator reports.
 *
 * <p>The verdict is the same in both modes — a table that fails one fails the other. What differs
 * is how much is known about why.
 */
public enum ValidationMode {

  /**
   * Report the first mismatch in a partition and stop, which is what the validator has always
   * done. Cheapest, and its exception text is relied on by existing consumers.
   */
  FAIL_FAST("fail-fast"),

  /**
   * Compile every mismatching file in every partition and write them to storage before failing the
   * validation. Costs a listing comparison that cannot exit early, plus one read per storage-side
   * log file to establish its instant.
   */
  EXHAUSTIVE("exhaustive");

  private final String flagValue;

  ValidationMode(String flagValue) {
    this.flagValue = flagValue;
  }

  public String getFlagValue() {
    return flagValue;
  }

  /**
   * Parses the {@code --validation-mode} flag, accepting either spelling ({@code fail-fast} or
   * {@code FAIL_FAST}) and rejecting anything else.
   *
   * <p>Rejects rather than defaulting: a typo silently falling back to fail-fast would leave an
   * operator waiting for a findings file that is never going to be written.
   */
  public static ValidationMode fromFlagValue(String value) {
    if (value == null || value.trim().isEmpty()) {
      return FAIL_FAST;
    }
    String normalized = value.trim().replace('-', '_');
    return Arrays.stream(values())
        .filter(mode -> mode.name().equalsIgnoreCase(normalized))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Unknown --validation-mode '" + value + "'; supported values are "
                + Arrays.stream(values()).map(ValidationMode::getFlagValue)
                    .collect(Collectors.joining(", "))));
  }
}
