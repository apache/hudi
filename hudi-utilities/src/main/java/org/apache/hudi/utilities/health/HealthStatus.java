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

/**
 * Outcome of a single {@link HealthCheck}.
 *
 * <p>Ordered from least to most severe so that a table-level verdict can be derived by taking the
 * maximum over the individual check outcomes; see {@link #max(HealthStatus, HealthStatus)}.
 */
public enum HealthStatus {
  /**
   * The check did not run. Either the governing writer configuration was not supplied, or the check
   * does not apply to this table (for example, compaction on a Copy-on-Write table). Never a failure:
   * a skipped check is silent about the table's health rather than asserting it is fine.
   */
  SKIPPED,

  /** The check ran and the table service is keeping up. */
  HEALTHY,

  /** The check ran and found the table service falling behind. */
  UNHEALTHY;

  public static HealthStatus max(HealthStatus left, HealthStatus right) {
    return left.compareTo(right) >= 0 ? left : right;
  }

  public boolean isUnhealthy() {
    return this == UNHEALTHY;
  }
}
