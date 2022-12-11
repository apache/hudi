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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Hudi Console metrics reporter. Reports the metrics by printing them to the stdout on the console.
 */
public class ConsoleMetricsReporter extends MetricsReporter {

  private static final Logger LOG = LogManager.getLogger(ConsoleMetricsReporter.class);
  private final ConsoleReporter consoleReporter;

  public ConsoleMetricsReporter(MetricRegistry registry) {
    this.consoleReporter = ConsoleReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL).build();
  }

  @Override
  public void start() {
    if (consoleReporter != null) {
      consoleReporter.start(30, TimeUnit.SECONDS);
    } else {
      LOG.error("Cannot start as the consoleReporter is null.");
    }
  }

  @Override
  public void report() {
    if (consoleReporter != null) {
      consoleReporter.report();
    } else {
      LOG.error("Cannot report metrics as the consoleReporter is null.");
    }
  }

  @Override
  public void stop() {
    if (consoleReporter != null) {
      consoleReporter.stop();
    }
  }
}
