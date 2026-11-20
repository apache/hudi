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

package org.apache.hudi.utilities.util.archive;

import java.util.Objects;

/**
 * Whether a file's scan managed to read everything it should have.
 *
 * <p>A damaged outcome does not say <em>which</em> instants were lost -- a corrupt block is corrupt
 * precisely because its contents cannot be read. The searcher therefore poisons the file's whole
 * probed range, or every instant if the range was never established.
 */
public final class ScanOutcome {

  private static final ScanOutcome CLEAN = new ScanOutcome(true, null);

  private final boolean clean;
  private final String reason;

  private ScanOutcome(boolean clean, String reason) {
    this.clean = clean;
    this.reason = reason;
  }

  /** Every block in the file was read. Absence of a target in this file is now a fact. */
  public static ScanOutcome clean() {
    return CLEAN;
  }

  /**
   * Part of the file could not be read.
   *
   * @param reason what went wrong, for the operator log; must not be null
   */
  public static ScanOutcome damaged(String reason) {
    return new ScanOutcome(false, Objects.requireNonNull(reason, "reason"));
  }

  public boolean isClean() {
    return clean;
  }

  /** Null when clean. */
  public String reason() {
    return reason;
  }

  @Override
  public String toString() {
    return clean ? "clean" : "damaged: " + reason;
  }
}
