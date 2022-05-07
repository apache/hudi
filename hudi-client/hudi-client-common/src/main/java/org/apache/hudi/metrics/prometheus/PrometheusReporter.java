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

package org.apache.hudi.metrics.prometheus;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.MetricsReporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;

/**
 * Implementation of Prometheus reporter, which connects to the Http server, and get metrics
 * from that server.
 */
public class PrometheusReporter extends MetricsReporter {

  private static final Logger LOG = LogManager.getLogger(PrometheusReporter.class);

  private HTTPServer httpServer;
  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;

  public PrometheusReporter(HoodieWriteConfig config, MetricRegistry registry) {
    int serverPort = config.getPrometheusPort();
    collectorRegistry = new CollectorRegistry();
    metricExports = new DropwizardExports(registry);
    metricExports.register(collectorRegistry);
    try {
      httpServer = new HTTPServer(new InetSocketAddress(serverPort), collectorRegistry);
    } catch (Exception e) {
      String msg = "Could not start PrometheusReporter HTTP server on port " + serverPort;
      LOG.error(msg, e);
      throw new HoodieException(msg, e);
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
  }

  @Override
  public Closeable getReporter() {
    return null;
  }

  @Override
  public void stop() {
    collectorRegistry.unregister(metricExports);
    if (httpServer != null) {
      httpServer.stop();
    }
  }
}
