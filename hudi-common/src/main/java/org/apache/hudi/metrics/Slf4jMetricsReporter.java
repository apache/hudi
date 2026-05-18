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

package org.apache.hudi.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * Hudi Slf4j metrics reporter. Reports the metrics by printing them to the log.
 */
@Slf4j
public class Slf4jMetricsReporter extends MetricsReporter {

  private final Slf4jReporter reporter;

  public Slf4jMetricsReporter(MetricRegistry registry) {
    this.reporter = Slf4jReporter.forRegistry(registry)
        .outputTo(log)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build();
  }

  @Override
  public void start() {
    if (reporter != null) {
      reporter.start(30, TimeUnit.SECONDS);
    } else {
      log.error("Cannot start as the reporter is null.");
    }
  }

  @Override
  public void report() {
    if (reporter != null) {
      reporter.report();
    } else {
      log.error("Cannot report metrics as the reporter is null.");
    }
  }

  @Override
  public void stop() {
    if (reporter != null) {
      reporter.stop();
    } else {
      log.error("Cannot stop as the reporter is null.");
    }
  }
}
