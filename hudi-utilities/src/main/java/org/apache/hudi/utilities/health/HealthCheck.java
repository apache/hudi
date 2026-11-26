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
 * One diagnosis of whether a table service is keeping up.
 *
 * <p>Implementations are read-only: they inspect timeline and file-system state and report, never
 * scheduling or running a table service. Each is independently selectable from the command line via
 * {@link #getName()}, so an operator can run the whole battery daily or a single check while
 * investigating.
 *
 * <p>An implementation should not throw to signal an unhealthy table -- that is what
 * {@link HealthStatus#UNHEALTHY} is for. Exceptions are reserved for a check that could not reach a
 * verdict at all, and the runner records those separately so one broken check does not hide the
 * results of the others.
 */
public interface HealthCheck {

  /** Command-line name of this check, as accepted by {@code --checks}. Lowercase, hyphenated. */
  String getName();

  /** One line describing what this check looks at, shown in {@code --help}. */
  String getDescription();

  /**
   * Whether this check applies to the given table at all, independent of configuration. A check
   * that does not apply -- compaction on a Copy-on-Write table, for instance -- is reported as
   * {@link HealthStatus#SKIPPED} rather than run.
   */
  default boolean appliesTo(HealthCheckContext context) {
    return true;
  }

  /** Runs the check and returns its verdict. */
  HealthCheckResult check(HealthCheckContext context) throws Exception;
}
