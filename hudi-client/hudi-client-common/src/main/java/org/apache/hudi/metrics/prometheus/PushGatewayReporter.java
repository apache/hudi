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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PushGatewayReporter extends ScheduledReporter {

  private static final Logger LOG = LogManager.getLogger(PushGatewayReporter.class);
  // Ensures that we maintain a single PushGw client (single connection pool) per Push Gw Server instance.
  private static final Map<String, PushGateway> PUSH_GATEWAY_PER_HOSTNAME = new ConcurrentHashMap<>();

  private final PushGateway pushGatewayClient;
  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;
  private final String jobName;
  private final Map<String, String> labels;
  private final boolean deleteShutdown;

  protected PushGatewayReporter(MetricRegistry registry,
                                MetricFilter filter,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                String jobName,
                                Map<String, String> labels,
                                String serverHost,
                                int serverPort,
                                boolean deleteShutdown) {
    super(registry, "hudi-push-gateway-reporter", filter, rateUnit, durationUnit);
    this.jobName = jobName;
    this.labels = labels;
    this.deleteShutdown = deleteShutdown;
    collectorRegistry = new CollectorRegistry();
    metricExports = new DropwizardExports(registry);
    pushGatewayClient = createPushGatewayClient(serverHost, serverPort);
    metricExports.register(collectorRegistry);
  }

  private synchronized PushGateway createPushGatewayClient(String serverHost, int serverPort) {
    final String serverUrl = String.format("%s:%s", serverHost, serverPort);
    if (PUSH_GATEWAY_PER_HOSTNAME.containsKey(serverUrl)) {
      return PUSH_GATEWAY_PER_HOSTNAME.get(serverUrl);
    }

    PushGateway pushGateway;
    if (serverPort == 443) {
      try {
        pushGateway = new PushGateway(new URL("https://" + serverUrl));
      } catch (MalformedURLException e) {
        e.printStackTrace();
        throw new IllegalArgumentException("Malformed pushgateway host: " + serverHost);
      }
    } else {
      pushGateway = new PushGateway(serverUrl);
    }
    PUSH_GATEWAY_PER_HOSTNAME.put(serverUrl, pushGateway);
    return pushGateway;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    try {
      pushGatewayClient.pushAdd(collectorRegistry, jobName, labels);
    } catch (IOException e) {
      LOG.warn("Can't push monitoring information to pushGateway", e);
    }
  }

  @Override
  public void start(long period, TimeUnit unit) {
    super.start(period, unit);
  }

  @Override
  public void stop() {
    super.stop();
    try {
      if (deleteShutdown) {
        collectorRegistry.unregister(metricExports);
        pushGatewayClient.delete(jobName);
      }
    } catch (IOException e) {
      LOG.warn("Failed to delete metrics from pushGateway with jobName {" + jobName + "}", e);
    }
  }
}
