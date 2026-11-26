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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Everything a {@link HealthCheck} is given: the table, the writer properties it is judged against,
 * and whether unset properties may fall back to Hudi defaults.
 *
 * <p>The distinction that {@code applyAllDefaults} draws is the point of this class. A health verdict
 * is derived by comparing observed table state against what the writer configuration says should
 * happen -- how many delta commits before compaction triggers, how many commits the cleaner retains,
 * how many instants archival keeps. Hudi supplies a default for every one of those, so a checker can
 * always produce an answer. But when the table is actually written with different settings, that
 * answer is wrong, and wrong in the worst direction: a table that is perfectly healthy under its own
 * configuration gets reported as UNHEALTHY, and an operator learns to distrust the tool.
 *
 * <p>So by default a check whose governing property was not supplied reports
 * {@link HealthStatus#SKIPPED} rather than guessing. Operators who know their table runs on stock
 * settings, or who just want a rough look, opt into defaults explicitly with
 * {@code --apply-all-defaults}.
 */
public class HealthCheckContext {

  private final HoodieTableMetaClient metaClient;
  private final TypedProperties props;
  private final boolean applyAllDefaults;
  private final Option<JavaSparkContext> jsc;

  public HealthCheckContext(HoodieTableMetaClient metaClient, TypedProperties props, boolean applyAllDefaults) {
    this(metaClient, props, applyAllDefaults, Option.empty());
  }

  public HealthCheckContext(HoodieTableMetaClient metaClient, TypedProperties props, boolean applyAllDefaults,
                            Option<JavaSparkContext> jsc) {
    this.metaClient = metaClient;
    this.props = props;
    this.applyAllDefaults = applyAllDefaults;
    this.jsc = jsc;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  /**
   * The Spark context, when the driver was given one.
   *
   * <p>Checks that only read the timeline do not need it. Two kinds do: one delegating to a
   * Spark-based tool, as the metadata-table sync check does with
   * {@code HoodieMetadataTableValidator}, and one whose work does not fit on the driver -- walking
   * file slices per file group for compaction, or file versions for cleaning, is a driver-side loop
   * today and will not stay one on a table with hundreds of thousands of file groups.
   *
   * <p>A check that needs it must handle its absence by reporting {@link HealthStatus#SKIPPED} and
   * saying what it needed, rather than failing the run: the checker is meant to stay usable
   * without a cluster for the checks that do not require one.
   */
  public Option<JavaSparkContext> getJsc() {
    return jsc;
  }

  public TypedProperties getProps() {
    return props;
  }

  public boolean isApplyAllDefaults() {
    return applyAllDefaults;
  }

  /**
   * Whether every one of {@code keys} was supplied by the operator, or defaults were explicitly
   * allowed. Checks call this before reading configuration so that a missing key becomes a
   * {@link HealthStatus#SKIPPED} verdict instead of a verdict derived from a value the table may
   * never have been written with.
   */
  public boolean hasConfigsOrDefaultsAllowed(String... keys) {
    if (applyAllDefaults) {
      return true;
    }
    for (String key : keys) {
      if (!props.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  /**
   * The first of {@code keys} that the operator did not supply, for naming in a skip reason.
   * Empty when all are present or defaults are allowed.
   */
  public Option<String> firstMissingConfig(String... keys) {
    if (applyAllDefaults) {
      return Option.empty();
    }
    for (String key : keys) {
      if (!props.containsKey(key)) {
        return Option.of(key);
      }
    }
    return Option.empty();
  }

  /**
   * A skip result naming the missing property and how to proceed. Phrased for an operator reading
   * the report, who needs to know both what to supply and that there is a way to run without it.
   */
  public HealthCheckResult skipForMissingConfig(String checkName, String missingKey) {
    return HealthCheckResult.skipped(checkName, String.format(
        "Writer property '%s' was not supplied, so this check cannot judge the table against the "
            + "settings it is actually written with. Pass it via --props or --hoodie-conf, or rerun "
            + "with --apply-all-defaults to evaluate against Hudi defaults.", missingKey));
  }
}
