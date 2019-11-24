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

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.MetricsSystem;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Prometheus PushGateWay reporter, which connects to the Prometheus server, and send metrics
 * to that server.
 */
public class PushGatewayReporter extends MetricsReporter {

  private static final Logger LOG = LogManager.getLogger(PushGatewayReporter.class);
  private PushGateWayServer pushGateWayServer;

  public PushGatewayReporter(HoodieWriteConfig config, MetricRegistry registry) {
    // Check the serverHost and serverPort here
    String serverHost = config.getPrometheusPushGatewayHost();
    int serverPort = config.getPrometheusPushGatewayPort();
    if (serverHost == null || serverPort == 0) {
      throw new RuntimeException(
          String.format("Prometheus cannot be initialized with serverHost[%s] and serverPort[%s].",
              serverHost, serverPort));
    }
    MetricsSystem.checkMinimalPollingPeriod(TimeUnit.SECONDS, 30);
    pushGateWayServer = new PushGateWayServer(serverHost, serverPort, registry);
  }

  @Override
  public void start() {
    if (pushGateWayServer != null) {
      pushGateWayServer.start();
    } else {
      LOG.error("Cannot start as the PushGatewayReporter is null.");
    }
  }

  @Override
  public void report() {
    try {
      if (pushGateWayServer != null) {
        pushGateWayServer.report();
      }
    } catch (Exception e) {
      throw new HoodieException("PushGateWayServer report failed: ", e);
    }
  }

  @Override
  public Closeable getReporter() {
    return null;
  }

  @Override
  public void stop() {
    if (pushGateWayServer != null) {
      try {
        pushGateWayServer.stop();
      } catch (IOException e) {
        LOG.error("Failed to stop PushGateWay server.", e);
      }
    }
  }

  private static class PushGateWayServer {

    private PushGateway pushGateway;
    private CollectorRegistry pushRegistry;
    private DropwizardExports sparkMetricExports;
    private String sparkAppId;

    public PushGateWayServer(String host, int port, MetricRegistry registry) {
      pushRegistry = new CollectorRegistry();
      sparkMetricExports = new DropwizardExports(registry);
      pushGateway = new PushGateway(host + ":" + port);
      sparkAppId = SparkEnv.get().conf().getAppId();
    }

    public void start() {
      sparkMetricExports.register(pushRegistry);
    }

    public void stop() throws IOException {
      pushRegistry.unregister(sparkMetricExports);
      pushGateway.delete(sparkAppId);
    }

    public void report() throws IOException {
      pushGateway.pushAdd(pushRegistry, sparkAppId);
    }
  }
}
