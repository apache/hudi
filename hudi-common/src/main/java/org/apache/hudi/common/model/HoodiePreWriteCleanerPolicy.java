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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Policy for running clean and/or rollback of failed writes before starting a new ingestion commit
 * (when startCommit or startCommitWithTime is invoked).
 */
@EnumDescription("If set, force attempting clean and/or failed writes rollback before starting a new ingestion write commit. "
    + "This should only be set to ensure that data files do not build up on DFS if an ingestion "
    + "writer is perpetually failing before completing a CLEAN.")
public enum HoodiePreWriteCleanerPolicy {

  @EnumFieldDescription("No pre-write clean or rollback. Default behavior.")
  NONE,

  @EnumFieldDescription("Force a CLEAN table service call before starting the write (also performs rollback of failed writes).")
  CLEAN,

  @EnumFieldDescription("Only perform rollback of failed writes before starting the write.")
  ROLLBACK_FAILED_WRITES;

  public boolean isNone() {
    return this == NONE;
  }

  public boolean isClean() {
    return this == CLEAN;
  }

  public boolean isRollbackFailedWrites() {
    return this == ROLLBACK_FAILED_WRITES;
  }

  /**
   * Parses the config value into an enum. Supports enum names (NONE, CLEAN, ROLLBACK_FAILED_WRITES)
   * and legacy string values ("clean", "rollback_failed_writes", empty/null for NONE).
   */
  public static HoodiePreWriteCleanerPolicy fromString(String value) {
    if (value == null || value.isEmpty()) {
      return NONE;
    }
    String normalized = value.trim();
    if (normalized.isEmpty()) {
      return NONE;
    }
    if ("rollback_failed_writes".equalsIgnoreCase(normalized)) {
      return ROLLBACK_FAILED_WRITES;
    }
    if ("clean".equalsIgnoreCase(normalized)) {
      return CLEAN;
    }
    return HoodiePreWriteCleanerPolicy.valueOf(normalized.toUpperCase());
  }
}
