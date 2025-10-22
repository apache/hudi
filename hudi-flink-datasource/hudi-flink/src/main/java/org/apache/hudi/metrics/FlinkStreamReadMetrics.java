/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;

/**
 * Metrics for flink stream read.
 */
public class FlinkStreamReadMetrics extends HoodieFlinkMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamReadMetrics.class);

  /**
   * The last issued instant in streaming read.
   *
   * @see StreamReadMonitoringFunction
   */
  private long issuedInstant;

  /**
   * Duration between last issued instant and now.
   */
  private long issuedInstantDelay;

  /**
   * Latest commit of current read split.
   *
   * @see StreamReadOperator
   */
  private long splitLatestCommit;

  /**
   * Duration between splitLatestCommit and now.
   */
  private long splitLatestCommitDelay;
  /**
   * Number of corrupt log file which will be skipped when stream read
   */
  private long totalCorruptLogFiles;

  public FlinkStreamReadMetrics(MetricGroup metricGroup) {
    super(metricGroup);
  }

  @Override
  public void registerMetrics() {
    metricGroup.gauge("issuedInstantDelay", () -> issuedInstantDelay);
    metricGroup.gauge("issuedInstant", () -> issuedInstant);
    metricGroup.gauge("splitLatestCommit", () -> splitLatestCommit);
    metricGroup.gauge("splitLatestCommitDelay", () -> splitLatestCommitDelay);
    metricGroup.gauge("totalCorruptLogFiles", () -> totalCorruptLogFiles);
  }

  public void setIssuedInstant(String issuedInstant) {
    try {
      Instant instant = HoodieInstantTimeGenerator.parseDateFromInstantTime(issuedInstant).toInstant();
      this.issuedInstant = instant.getEpochSecond();
      this.issuedInstantDelay = Duration.between(instant, Instant.now()).getSeconds();
    } catch (ParseException e) {
      LOG.warn("Invalid input issued instant: {}", issuedInstant);
    }
  }

  public void setSplitLatestCommit(String splitLatestCommit) {
    try {
      Instant instant = HoodieInstantTimeGenerator.parseDateFromInstantTime(splitLatestCommit).toInstant();
      this.splitLatestCommit = instant.getEpochSecond();
      this.splitLatestCommitDelay = Duration.between(instant, Instant.now()).getSeconds();
    } catch (ParseException e) {
      LOG.warn("Invalid input latest commit: {}", splitLatestCommit);
    }
  }

  public void setTotalCorruptLogFiles(long totalCorruptLogFiles) {
    this.totalCorruptLogFiles = totalCorruptLogFiles;
  }

}
