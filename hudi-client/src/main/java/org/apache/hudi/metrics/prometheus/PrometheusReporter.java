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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.MetricsReporter;

import io.prometheus.client.exporter.HTTPServer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;

/**
 * Implementation of Prometheus reporter, which connects to the Http server, and get metrics
 * from that server.
 */
public class PrometheusReporter extends MetricsReporter {

  private static final Logger LOG = LogManager.getLogger(PrometheusReporter.class);

  private HTTPServer httpServer;

  public PrometheusReporter(HoodieWriteConfig config) {
    // Check the serverHost and serverPort here
    String serverHost = config.getPrometheusHost();
    int serverPort = config.getPrometheusPort();
    if (serverHost == null || serverPort == 0) {
      throw new RuntimeException(
          String.format("Prometheus cannot be initialized with serverHost[%s] and serverPort[%s].",
              serverHost, serverPort));
    }
    try {
      httpServer = new HTTPServer(serverHost, serverPort);
    } catch (Exception e) {
      String msg = "Could not start PrometheusReporter HTTP server on port ";
      LOG.error(msg + serverPort, e);
      throw new HoodieException(msg + serverPort, e);
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
    if (httpServer != null) {
      httpServer.stop();
    }
  }
}
