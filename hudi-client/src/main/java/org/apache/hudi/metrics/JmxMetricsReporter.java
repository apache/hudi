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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import java.io.Closeable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Implementation of Jmx reporter, which used to report jmx metric.
 */
public class JmxMetricsReporter extends MetricsReporter {
  private static Logger logger = LogManager.getLogger(JmxMetricsReporter.class);

  private final MetricRegistry registry;
  private final JmxReporter jmxReporter;

  public JmxMetricsReporter(MetricRegistry registry) {
    this.registry = registry;
    this.jmxReporter = createJmxReport();
  }

  @Override
  public void start() {
    if (jmxReporter != null) {
      jmxReporter.start();
    } else {
      logger.error("Cannot start as the jmxReporter is null.");
    }
  }

  @Override public void report() {

  }

  @Override
  public Closeable getReporter() {
    return jmxReporter;
  }

  private JmxReporter createJmxReport() {
    return JmxReporter.forRegistry(registry).build();
  }
}
