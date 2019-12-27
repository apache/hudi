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

import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Graphite reporter, which connects to the Graphite server, and send metrics to that server.
 */
public class MetricsGraphiteReporter extends MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsGraphiteReporter.class);
  private final MetricRegistry registry;
  private final GraphiteReporter graphiteReporter;
  private final HoodieWriteConfig config;
  private String serverHost;
  private int serverPort;

  public MetricsGraphiteReporter(HoodieWriteConfig config, MetricRegistry registry) {
    this.registry = registry;
    this.config = config;

    // Check the serverHost and serverPort here
    this.serverHost = config.getGraphiteServerHost();
    this.serverPort = config.getGraphiteServerPort();
    if (serverHost == null || serverPort == 0) {
      throw new RuntimeException(String.format("Graphite cannot be initialized with serverHost[%s] and serverPort[%s].",
          serverHost, serverPort));
    }

    this.graphiteReporter = createGraphiteReport();
  }

  @Override
  public void start() {
    if (graphiteReporter != null) {
      graphiteReporter.start(30, TimeUnit.SECONDS);
    } else {
      LOG.error("Cannot start as the graphiteReporter is null.");
    }
  }

  @Override
  public void report() {
    if (graphiteReporter != null) {
      graphiteReporter.report();
    } else {
      LOG.error("Cannot report metrics as the graphiteReporter is null.");
    }
  }

  @Override
  public Closeable getReporter() {
    return graphiteReporter;
  }

  private GraphiteReporter createGraphiteReport() {
    Graphite graphite = new Graphite(new InetSocketAddress(serverHost, serverPort));
    String reporterPrefix = config.getGraphiteMetricPrefix();
    return GraphiteReporter.forRegistry(registry).prefixedWith(reporterPrefix).convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build(graphite);
  }
}
