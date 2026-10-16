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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Verdict of one {@link HealthCheck} over one table.
 *
 * <p>Carries three things beyond the {@link HealthStatus}: a one-line {@code summary} for the
 * operator, the {@code effectiveConfigs} the check actually reasoned with, and {@code findings}
 * naming what is behind. Echoing the configuration back matters because a verdict derived from
 * writer properties is only meaningful alongside the properties it used -- the same table is
 * healthy or unhealthy depending on the retention and trigger settings it is written with.
 */
public class HealthCheckResult {

  private final String checkName;
  private final HealthStatus status;
  private final String summary;
  private final Map<String, String> effectiveConfigs;
  private final List<String> findings;

  private HealthCheckResult(String checkName, HealthStatus status, String summary,
                            Map<String, String> effectiveConfigs, List<String> findings) {
    this.checkName = checkName;
    this.status = status;
    this.summary = summary;
    this.effectiveConfigs = Collections.unmodifiableMap(new LinkedHashMap<>(effectiveConfigs));
    this.findings = Collections.unmodifiableList(new ArrayList<>(findings));
  }

  public static Builder builder(String checkName) {
    return new Builder(checkName);
  }

  /** A check that did not run, with the reason stated for the operator. */
  public static HealthCheckResult skipped(String checkName, String reason) {
    return builder(checkName).status(HealthStatus.SKIPPED).summary(reason).build();
  }

  public String getCheckName() {
    return checkName;
  }

  public HealthStatus getStatus() {
    return status;
  }

  public String getSummary() {
    return summary;
  }

  public Map<String, String> getEffectiveConfigs() {
    return effectiveConfigs;
  }

  public List<String> getFindings() {
    return findings;
  }

  public static class Builder {
    private final String checkName;
    private final Map<String, String> effectiveConfigs = new LinkedHashMap<>();
    private final List<String> findings = new ArrayList<>();
    private HealthStatus status = HealthStatus.SKIPPED;
    private String summary = "";

    private Builder(String checkName) {
      this.checkName = checkName;
    }

    public Builder status(HealthStatus status) {
      this.status = status;
      return this;
    }

    public Builder summary(String summary) {
      this.summary = summary;
      return this;
    }

    public Builder config(String key, Object value) {
      this.effectiveConfigs.put(key, String.valueOf(value));
      return this;
    }

    public Builder finding(String finding) {
      this.findings.add(finding);
      return this;
    }

    public HealthCheckResult build() {
      return new HealthCheckResult(checkName, status, summary, effectiveConfigs, findings);
    }
  }
}
